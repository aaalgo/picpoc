#include <algorithm>
#include <unordered_map>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include "picpoc.h"

namespace picpoc {

    namespace fs = boost::filesystem;
    using boost::lexical_cast;

    void DirectFile::load (string const &path, vector<unique_ptr<Container>> *v) {
        DirectFile file(path, MODE_READ);
        v->clear();
        for (;;) {
            char *buf;
            size_t size;
            try {
                file.alloc_read(&buf, &size);
                v->push_back(std::move(make_unique<Container>(buf, size)));
            }
            catch (EoS const &) {
                break;
            }
        }
    }

    void DirectFile::shuffle (string const &in_path, string const &out_path) {
        vector<unique_ptr<Container>> all;
        load(in_path, &all);
        unsigned total = 0;
        size_t cs = 0;
        for (auto const &c: all) {
            total += c->size();
            cs = std::max(cs, c->packed_size());
        }
        vector<std::pair<unsigned, unsigned>> index;
        index.reserve(total);
        for (unsigned i = 0; i < all.size(); ++i) {
            for (unsigned j = 0; j < all[i]->size(); ++j) {
                index.emplace_back(i, j);
            }
        }
        std::random_shuffle(index.begin(), index.end());
        DirectFile out(out_path, MODE_WRITE);
        unique_ptr<Container> c = make_unique<Container>(cs);
        for (auto const &p: index) {
            for (unsigned i = 0; i < 2; ++i) {
                if (c->add(all[p.first]->at(p.second))) break;
                out.write_free(std::move(c));
                c = make_unique<Container>(cs);
            }
        }
        if (c->size()) {
            out.write_free(std::move(c));
        }
    }

    void DataSet::rotate (string const &input_dir, string const &output_dir, size_t n_stream) {
        DataSet::Info ds_info;
        DataSet::ping(input_dir, &ds_info);
        if (n_stream == 0) {
            n_stream = ds_info.subs.size();
        }
        vector<vector<string>> jobs(n_stream);
        unsigned next = 0;
        fs::path in_root(input_dir);
        for (int st: ds_info.subs) {
            fs::path st_path = in_root/lexical_cast<string>(st);
            Stream::Info st_info;
            Stream::ping(st_path.string(), &st_info);
            for (int f: st_info.subs) {
                fs::path path = st_path/lexical_cast<string>(f);
                string s = path.string();
                DirectFile::Info f_info;
                DirectFile::ping(s, &f_info);
                jobs[next].push_back(path.string());
                next = (next + 1) % jobs.size();
            }
        }
        fs::path out_root(output_dir);
        fs::create_directories(out_root);
        for (unsigned i = 0; i < jobs.size(); ++i) {
            auto const &v = jobs[i];
            fs::path st_path = out_root/lexical_cast<string>(i);
            fs::create_directory(st_path);
            for (unsigned j = 0; j < v.size(); ++j) {
                fs::path f_path = st_path/lexical_cast<string>(j);
                DirectFile::shuffle(v[j], f_path.string());
            }
        }
    }

    void count (string const &path, std::unordered_map<unsigned, int> *cnt, int v) {
        DataSet ds(path);
        for (;;) {
            Record rec;
            try {
                ds >> rec;
                (*cnt)[rec.meta.serial] += v;
            }
            catch (EoS const &) {
                break;
            }
        }
    }
    void DataSet::verify_content (string const &path1, string const &path2, bool io) {
        std::unordered_map<unsigned, int> x;
        count(path1, &x, 1);
        count(path2, &x, -1);
        for (auto const &p: x) {
            BOOST_VERIFY(p.second == 0);
        }
    }
}

