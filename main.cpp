#include <filesystem>
#include <string>
#include <iostream>
#include <fstream>
#include <functional>
#include <vector>
#include <unordered_map>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <string_view>
#include <chrono>

struct StringSeq
{
    StringSeq() = default;
    StringSeq(std::string & s, std::size_t _seq): 
        word(std::move(s)),
        seq(_seq) {}
    std::string word;
    size_t seq;
};

void splitFile(const std::string & infile, const std::vector<std::string> & outfiles);
void calcFirstUniqWord(const std::vector<std::string> & infiles, size_t start_idx, size_t end_idx, std::vector<StringSeq> & results);
void calcFirstUniqWordParallel(const std::vector<std::string> & infiles, size_t parallel_num, std::vector<StringSeq> & results);
StringSeq mergeResults(const std::vector<StringSeq> & results);

int main(int argc, char ** argv)
{
    std::string infile = argv[1];
    size_t parallel_num = std::stoull(argv[2]);
    size_t split_num = std::stoull(argv[3]);

    size_t middle_files_num = parallel_num * split_num;

    //生成间文件，第二阶段每个线程顺序处理split num个文件
    std::vector<std::string> middle_files(middle_files_num);
    for (size_t i = 0; i < middle_files.size(); ++i)
        middle_files[i] = "./" + std::to_string(i+1);

    //第一阶段: 将输入文件split成多个中间文件
    splitFile(infile, middle_files);

    //第二阶段: 并行处理所有的中间文件，并发算出每个中间文件中的第一个不重复的word
    //此处传入的results数组只在算出结果时才会写一次，不会导致严重的cache conflict
    std::vector<StringSeq> results(middle_files_num);
    calcFirstUniqWordParallel(middle_files, parallel_num, results);

    //第三阶段：合并结果
    StringSeq got = mergeResults(results);

    if (!got.word.empty())
        std::cout << "word:" << got.word << " seq:" << got.seq << std::endl;
    else
        std::cout << "not find" << std::endl;

    for (auto & f : middle_files)
        std::filesystem::remove(f);
}

struct WordSeq
{
    WordSeq() = default;
    WordSeq(char * data, size_t len, size_t _seq):
        word(data, len),
        seq(_seq) {}

    std::string_view word;
    size_t seq;
};

#define BUF_LEN 512*1024//512k

struct Context;
using ContextPtr = std::shared_ptr<Context>;

//数据结构设计考虑了局部性原理,数据尽量连续存储. 
//不直接使用string类，规避各种零碎、频繁的动态申请内存和内存拷贝
//之所以抽出来一个Context，主要是为了把一次读文件的上下文数据集中封装，
//方便分词阶段后续可能涉及到的并行化优化, 迫于时间和带来的代码复杂度，目前还没做
struct Context
{
    Context(size_t output_file_num) :
        per_file_buf(output_file_num),
        per_file_buf_pos(output_file_num)

    {
        buf.resize(BUF_LEN);

        //预分配可能出现的最多的word数空间
        word_seqs.reserve(BUF_LEN/2);

        for (size_t i = 0; i < per_file_buf.size(); ++i)
        {
            //一次分配充足的内存，以空间换取时间
            //为什么是9*BUF_LEN, 为了涵盖最极端的情况:每行一个字符, 预留16个字节存储word len和seq
            per_file_buf[i] = new char[17*BUF_LEN]();
            per_file_buf_pos[i] = per_file_buf[i];
        }
    }

    ~Context()
    {
        for (size_t i = 0; i < per_file_buf.size(); ++i)
            delete []per_file_buf[i];
    }

    //映射单词
    void mapWord(size_t mod_size, size_t * selector)
    {
        std::hash<std::string_view> hash;
        for (size_t i = 0; i < word_seqs.size(); ++i)
            selector[i] = hash(word_seqs[i].word) % mod_size;
    }

    //集中归拢数据
    void gatherData(size_t *selector)
    {
        //format: |word1_len|word1|seq1|word2_len|word2|seq2|...
        for (size_t i = 0; i < word_seqs.size(); ++i)
        {
            size_t idx = selector[i];

            (*(size_t*)(per_file_buf_pos[idx])) = word_seqs[i].word.size();
            memmove(per_file_buf_pos[idx] + sizeof(size_t), word_seqs[i].word.data(), word_seqs[i].word.size());
            (*(size_t*)(per_file_buf_pos[idx] + sizeof(size_t) + word_seqs[i].word.size())) = word_seqs[i].seq;

            per_file_buf_pos[idx] += 2*sizeof(size_t) + word_seqs[i].word.size();
        }

    }

    //写出数据到中间文件
    void writeData(const std::vector<int> & outs)
    {
        //这里还有优化空间，可以等buf满了再写入，会提升写文件效率，不过这样整体的代码复杂度会增加，所以暂时没做
        for (size_t i = 0; i < outs.size(); ++i)
        {
            write(outs[i], per_file_buf[i], per_file_buf_pos[i] - per_file_buf[i]);
            per_file_buf_pos[i] = per_file_buf[i];
        }
    }

    static ContextPtr newContext(size_t output_file_num);

    //读buffuer，每次都在buf的空间上，原地构建word
    //每次批量读出来的word都是连续存储的, cpu cache友好
    std::vector<char> buf;

    //记录在buf的内存中构建的word和seq
    std::vector<WordSeq> word_seqs;

    //输出缓冲区
    //每个中间文件对应一个下标
    std::vector<char*> per_file_buf;
    std::vector<char*> per_file_buf_pos;
};

ContextPtr Context::newContext(size_t output_file_num)
{
    return std::make_shared<Context>(output_file_num);
}

void splitFile(const std::string & infile, const std::vector<std::string> & outfiles)
{
    size_t outfiles_num = outfiles.size();

    //open所有中间文件
    std::vector<int> outs(outfiles_num);
    for (size_t i = 0; i < outs.size(); ++i)
    {
        std::filesystem::remove(outfiles[i]);
        outs[i] = open(outfiles[i].c_str(), O_WRONLY | O_CREAT, 777);
        if (outs[i] < 0)
        {
            std::cerr << "open file fail. file:" << outfiles[i] << " error:" << strerror(errno) <<  std::endl;
            exit(-1);
        }
    }

    //open输入文件
    int in = open(infile.c_str(), O_RDONLY);
    if (in < 0)
    {
        std::cerr << "open file fail. file:" << infile << " error:" << strerror(errno) <<  std::endl;
        exit(-1);
    }

    //循环批量读文件，并构建word、seq，批量hash写入到中间文件
    auto ctx = Context::newContext(outfiles.size());
    size_t seq = 0; //序列号，用来表示全局单词序列号
    size_t write_offset = 0; //buf可写入的位置偏移
    size_t readn = 0;
    while(readn = read(in, ctx->buf.data() + write_offset, BUF_LEN - write_offset), readn > 0)
    {
        ctx->word_seqs.resize(0);

        char * pos = ctx->buf.data();
        const char * end = ctx->buf.data() + write_offset + readn;
        char * cur_word = ctx->buf.data();
        while (pos < end)
        {
            //每行一个单词
            if (*pos == '\n')
            {
                ++seq;
                //判断是否为空行
                if (pos > cur_word)
                    ctx->word_seqs.emplace_back(cur_word, pos - cur_word, seq);

                cur_word = pos + 1;
            }

            ++pos;
        }

        // 将word映射到所属的中间文件下标
        size_t selector[BUF_LEN];
        ctx->mapWord(outfiles.size(), selector);

        //把单词收集到每个中间文件的写入buf中，连续存储
        ctx->gatherData(selector);

        //将所有文件的写入buf中的数据，写到对应的文件中
        ctx->writeData(outs);

        //回收buf的尾部数据
        memmove(ctx->buf.data(), cur_word, end - cur_word);

        //调整buf的偏移
        write_offset = end - cur_word;
    }

    close(in);

    for (int fd : outs)
        close(fd);
}

//并行计算每个文件中的第一个不重复的单词
void calcFirstUniqWordParallel(const std::vector<std::string> & infiles, size_t parallel_num, std::vector<StringSeq> & results)
{
    size_t step = infiles.size() / parallel_num;
    std::vector<std::shared_ptr<std::thread>> threads;
    for (size_t i = 0; i < infiles.size(); i += step)
    {
        size_t s = i;
        size_t e = std::min(i + step, infiles.size()) - 1;

        threads.emplace_back(std::make_shared<std::thread>([&infiles, &results, s, e] () {
                    calcFirstUniqWord(infiles, s, e, results);
                    }));
    }

    for (auto & t : threads)
        t->join();
}

StringSeq mergeResults(const std::vector<StringSeq> & results)
{
    StringSeq min;
    min.seq = UINT64_MAX;
    for (auto & r : results)
        if (r.seq != 0 && r.seq < min.seq)
            min = r;
    return min;
}

struct SeqCount
{
    size_t seq;
    size_t count;
};

void calcFirstUniqWord(const std::vector<std::string> & infiles, size_t start_idx, size_t end_idx, std::vector<StringSeq> & results)
{
    //读取数据，并且在buf上原地构建word
    //bufs资源复用，最后统一释放
    std::vector<char *> bufs;

    for (size_t i = start_idx; i <= end_idx; ++i)
    {
        std::unordered_map<std::string_view, SeqCount> words;
        words.reserve(10*1000*1000);

        std::vector<WordSeq> word_seqs;
        word_seqs.reserve(BUF_LEN/(2*sizeof(size_t)));

        if (bufs.size() == 0)
            bufs.push_back(new char[BUF_LEN]);

        size_t cur_buf_idx = 0;
        char * buf = bufs[cur_buf_idx];

        int in = open(infiles[i].c_str(), O_RDONLY);
        if (in < 0)
        {
            std::cerr << "open file fail. file:" << infiles[i] << " error:" << strerror(errno) <<  std::endl;
            exit(-1);
        }

        //file format: |word1_len|word1|seq1|word2_len|word2|seq2|...
        size_t write_offset = 0;
        int readn = 0;
        while(readn = read(in, buf + write_offset, BUF_LEN - write_offset), readn > 0)
        {
            word_seqs.resize(0);

            char * pos = buf;
            const char * end = buf + write_offset + (size_t)readn;
            size_t word_len = *(size_t*)pos;

            //为什么有3个sizeof(size_t)? why not 2?
            //为了在循环中消除一次if判断: if (pos + sizeof(size_t) < end)
            while(pos + 3*sizeof(size_t) + word_len <= end)
            {
                char * word = pos + sizeof(size_t);
                size_t * seq = (size_t*)(pos + sizeof(size_t) + word_len);
                
                word_seqs.emplace_back(pos + sizeof(size_t), word_len, *seq);

                pos += 2*sizeof(size_t) + word_len;
                word_len = *(size_t*)pos;
            }

            //上个循环可能会留下一条记录没有读完
            if (pos + 2*sizeof(size_t) + word_len <= end)
            {
                char * word = pos + sizeof(size_t);
                size_t * seq = (size_t*)(pos + sizeof(size_t) + word_len);
                word_seqs.emplace_back(pos + sizeof(size_t), word_len, *seq);
                pos += 2*sizeof(size_t) + word_len;
            }

            //切换buffer
            if (cur_buf_idx == bufs.size() - 1)
                bufs.push_back(new char[BUF_LEN]);
            ++cur_buf_idx;
            char * new_buf = bufs[cur_buf_idx];

            //回收尾部数据
            memmove(new_buf, pos, end - pos);
            write_offset = end - pos;
            buf = new_buf;

            //统计word频次
            for (auto & w : word_seqs)
            {
                auto it = words.find(w.word);
                if (it == words.end())
                {
                    SeqCount s;
                    s.seq = w.seq;
                    s.count = 1;
                    words[w.word] = s;
                }
                else
                    it->second.count += 1;
            }
        }

        close(in);

        StringSeq min;
        min.seq = UINT64_MAX;
        for (auto & it : words)
        {
            if (it.second.count == 1 && it.second.seq < min.seq)
            {
                min.word = it.first;
                min.seq = it.second.seq;
            }
        }

        results[i] = min;
    }

    for (char * buf : bufs)
        delete []buf;

    return;
}
