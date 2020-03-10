#include <filesystem>
#include <string>
#include <iostream>
#include <fstream>
#include <functional>
#include <vector>
#include <unordered_map>
#include <thread>

struct StringSeq
{
    StringSeq() = default;
    StringSeq(std::string & _s, std::size_t _seq): 
        str(std::move(_s)),
        seq(_seq) {}
    std::string str;
    size_t seq = UINT64_MAX;
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
        middle_files[i] = "/tmp/." + std::to_string(i);


    //split input file to many middle files
    splitFile(infile, middle_files);


    std::vector<StringSeq> results(middle_files_num);
    calcFirstWordParallel(middle_files, cpu_num, results);


    StringSeq got = mergeResults(results);

    if (!got.str.empty())
        std::cout << "word:" << got.str << " seq:" << got.seq << std::endl;
    else
        std::cout << "not find" << std::endl;


    //remove middle files
    for (auto & f : middle_files)
        std::filesystem::remove(f);
}


void splitFile(const std::string & infile, const std::vector<std::string> & outfiles)
{
    size_t outfiles_num = outfiles.size();

    std::vector<std::ofstream> outs(outfiles_num);
    for (size_t i = 0; i < outs.size(); ++i)
        outs[i].open(outfiles[i], std::ofstream::out);

    std::hash<std::string> hash_fn;

    size_t seq = 0;
    std::string word;

    std::ifstream in(infile);
    while(std::getline(in, word))
    {
        size_t idx = hash_fn(word) % (outfiles_num);
        ++seq;
        if (!word.empty())
            outs[idx] << word << '\t' << seq << std::endl; 
    }
    in.close();

    for (auto & o: outs)
        o.close();
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
        std::unordered_map<std::string, SeqCount> words;
        words.reserve(10*1000*1000);

        auto in = std::ifstream(infiles[i]);

        std::string line;
        while(std::getline(in, line))
        {
            size_t pos = line.find_first_of('\t');
            std::string seq_str = line.substr(pos);

            line.resize(pos);

            auto it = words.find(line);
            if (it == words.end())
            {
                SeqCount s;
                s.seq = std::stoull(seq_str);
                s.count = 1;
                words[line] = s;
            }
            else
                it->second.count += 1;
        }
        in.close();

        StringSeq min;
        for (auto & it : words)
        {
            if (it.second.count == 1 && it.second.seq < min.seq)
            {
                min.str = it.first;
                min.seq = it.second.seq;
            }
        }

        results[i] = min;
    }

    return;
}





