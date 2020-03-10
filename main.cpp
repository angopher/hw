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
void calcFirstWord(const std::vector<std::string> & infiles, size_t start_idx, size_t end_idx, std::vector<StringSeq> & results);
void calcFirstWordParallel(const std::vector<std::string> & infiles, size_t parallel_num, std::vector<StringSeq> & results);
StringSeq mergeResults(const std::vector<StringSeq> & results);


int main(int argc, char ** argv)
{
    std::string infile = argv[1];
    size_t cpu_num = std::stoull(argv[2]);
    size_t split_num = std::stoull(argv[3]);

    size_t middle_files_num = cpu_num * split_num;

    std::vector<std::string> middle_files(middle_files_num);
    for (size_t i = 0; i < middle_files.size(); ++i)
        middle_files[i] = "./" + std::to_string(i+1);


    //split input file to many middle files
    splitFile(infile, middle_files);


    std::vector<StringSeq> results(middle_files_num);
    calcFirstWordParallel(middle_files, cpu_num, results);


    StringSeq got = mergeResults(results);

    if (!got.word.empty())
        std::cout << "word:" << got.word << " seq:" << got.seq << std::endl;
    else
        std::cout << "not find" << std::endl;


    //remove middle files
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

void mapWordToFile(const std::vector<WordSeq> & word_seqs, size_t mod_value, size_t * values)
{
    std::hash<std::string_view> hash;
    for (size_t i = 0; i < word_seqs.size(); ++i)
    {
        values[i] = hash(word_seqs[i].word) % mod_value;
    }
}

#define BUF_LEN 40960 //4M


void splitFile(const std::string & infile, const std::vector<std::string> & outfiles)
{
    size_t outfiles_num = outfiles.size();

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


    size_t seq = 0; //word sequence number in raw file

    char buf[BUF_LEN]; //read buffer
    size_t write_offset = 0; //buf write offset

    std::vector<WordSeq> word_seqs;
    word_seqs.reserve(BUF_LEN);

    //write buffer for outfiles
    std::vector<std::array<char, 5*BUF_LEN>> per_file_buf(outfiles.size()); 
    std::vector<char*> per_file_buf_pos(outfiles.size());


    int in = open(infile.c_str(), O_RDONLY);
    size_t readn = 0;
    while(readn = read(in, buf + write_offset, BUF_LEN - write_offset), readn > 0)
    {
        char * pos = buf;
        const char * end = buf + write_offset + readn;
        char * cur_word = buf;
        while (pos < end)
        {
            //per word per line
            if (*pos == '\n')
            {
                ++seq;
                if (pos > cur_word)
                    word_seqs.emplace_back(cur_word, pos - cur_word, seq);

                cur_word = pos + 1;
            }

            ++pos;
        }

        size_t file_idxs[BUF_LEN];

        mapWordToFile(word_seqs,outfiles.size(), file_idxs);


        for (size_t i = 0; i < per_file_buf.size(); ++i)
            per_file_buf_pos[i] = per_file_buf[i].data();

        //gather data to write buffer
        //output file format: |word1_len|word1|seq1|word2_len|word2|seq2|...
        for (size_t i = 0; i < word_seqs.size(); ++i)
        {
            size_t idx = file_idxs[i];

            (*(size_t*)(per_file_buf_pos[idx])) = word_seqs[i].word.size();
            memmove(per_file_buf_pos[idx] + sizeof(size_t), word_seqs[i].word.data(), word_seqs[i].word.size());
            (*(size_t*)(per_file_buf_pos[idx] + sizeof(size_t) + word_seqs[i].word.size())) = word_seqs[i].seq;

            per_file_buf_pos[idx] += 2*sizeof(size_t) + word_seqs[i].word.size();
        }

        for (size_t i = 0; i < outfiles.size(); ++i)
            write(outs[i], per_file_buf[i].data(), per_file_buf_pos[i] - per_file_buf[i].data());

        //try recycle unused data
        memmove(buf, cur_word, end - cur_word);
        write_offset = end - cur_word;
        word_seqs.resize(0);
    }

    close(in);

    for (int fd : outs)
        close(fd);
}


void calcFirstWordParallel(const std::vector<std::string> & infiles, size_t parallel_num, std::vector<StringSeq> & results)
{
    size_t step = infiles.size() / parallel_num;
    std::vector<std::shared_ptr<std::thread>> threads;
    for (size_t i = 0; i < infiles.size(); i += step)
    {
        size_t s = i;
        size_t e = std::min(i + step, infiles.size()) - 1;

        threads.emplace_back(std::make_shared<std::thread>([&infiles, &results, s, e] () {
                    calcFirstWord(infiles, s, e, results);
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


void calcFirstWord(const std::vector<std::string> & infiles, size_t start_idx, size_t end_idx, std::vector<StringSeq> & results)
{
    for (size_t i = start_idx; i <= end_idx; ++i)
    {
        std::unordered_map<std::string_view, SeqCount> words;
        words.reserve(10*1000*1000);

        std::vector<WordSeq> word_seqs;
        word_seqs.reserve(BUF_LEN/(2*sizeof(size_t)));

        std::vector<char *> bufs; //read buffer
        bufs.push_back(new char[BUF_LEN]); //use std::array<char, BUF_LEN>
        char * buf = bufs.back();


        int in = open(infiles[i].c_str(), O_RDONLY);
        if (in < 0)
        {
            std::cerr << "open file fail. file:" << infiles[i] << " error:" << strerror(errno) <<  std::endl;
            exit(-1);
        }

        size_t write_offset = 0; //buf write offset
        int readn = 0; // read bytes
        //output file format: |word1_len|word1|seq1|word2_len|word2|seq2|...
        while(readn = read(in, buf + write_offset, BUF_LEN - write_offset), readn > 0)
        {
            word_seqs.resize(0);

            char * pos = buf;
            const char * end = buf + write_offset + (size_t)readn;

            size_t word_len = *(size_t*)pos;

            //为什么有3个sizeof(size_t)? why not 2? 为了在循环中消除一次if判断
            while(pos + 3*sizeof(size_t) + word_len <= end)
            {
                char * word = pos + sizeof(size_t);
                size_t * seq = (size_t*)(pos + sizeof(size_t) + word_len);
                
                word_seqs.emplace_back(pos + sizeof(size_t), word_len, *seq);

                //std::cout << word_seqs.back().word << " " << word_seqs.back().seq << std::endl;

                pos += 2*sizeof(size_t) + word_len;
                word_len = *(size_t*)pos;
            }

            bufs.push_back(new char[BUF_LEN]);

            if (pos + 2*sizeof(size_t) + word_len <= end)
            {
                char * word = pos + sizeof(size_t);
                size_t * seq = (size_t*)(pos + sizeof(size_t) + word_len);
                word_seqs.emplace_back(pos + sizeof(size_t), word_len, *seq);
                pos += 2*sizeof(size_t) + word_len;
            }

            //std::cout << "write_offset:" << write_offset  << " word size:" << word_seqs.size() << std::endl;

            //recycle unused data
            memmove(bufs.back(), pos, end - pos);
            write_offset = end - pos;
            buf = bufs.back();


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
            //std::cout << "word:" << it.first << " count:" << it.second.count << " seq:" << it.second.seq << std::endl;
            if (it.second.count == 1 && it.second.seq < min.seq)
            {
                min.word = it.first;
                min.seq = it.second.seq;
            }
        }

        results[i] = min;

        for (char * buf : bufs)
            delete []buf;
    }

    return;
}





