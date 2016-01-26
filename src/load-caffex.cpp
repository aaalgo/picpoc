#include <atomic>
#include <iostream>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <boost/timer/timer.hpp>
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <opencv2/opencv.hpp>
#include <json11.hpp>
#include "picpoc.h"

using namespace std;
using namespace boost;
using namespace picpoc;
namespace fs = boost::filesystem;
using json11::Json;

class Paths: public vector<fs::path> {
public:
    Paths () {}
    Paths (fs::path const &path) {
        fs::recursive_directory_iterator it(fs::path(path), fs::symlink_option::recurse), end;
        for (; it != end; ++it) {
            if (it->status().type() == fs::regular_file) {
                push_back(it->path());
            }
        }
        CHECK(size() >= 10) << "Need at least 10 files to train: " << path;
        random_shuffle(this->begin(), this->end());
    }
};

struct Line {
    int label;
    fs::path path;
    Line () {}
    Line (int l, fs::path const &p): label(l), path(p) {
    }
};

class Samples: public vector<Paths> {
public:
    Samples () {}
    Samples (fs::path const &root) {
        fs::directory_iterator it(root), end;
        vector<unsigned> cats;
        for (; it != end; ++it) {
            if (!fs::is_directory(it->path())) {
                LOG(ERROR) << "Not a directory: " << it->path();
                continue;
            }
            fs::path name = it->path().filename();
            try {
                unsigned c = lexical_cast<unsigned>(name.native());
                cats.push_back(c);
            }
            catch (...) {
                LOG(ERROR) << "Category directory not properly named: " << it->path();
            }
        }
        sort(cats.begin(), cats.end());
        cats.resize(unique(cats.begin(), cats.end()) - cats.begin());
        CHECK(cats.size() >= 2) << "Need at least 2 categories to train.";
        CHECK((cats.front() == 0)
                && (cats.back() == cats.size() -1 ))
            << "Subdirectories must be consecutively named from 0 to N-1.";
        for (unsigned c = 0; c < cats.size(); ++c) {
            emplace_back(root / fs::path(lexical_cast<string>(c)));
            LOG(INFO) << "Loaded " << back().size() << " paths for category " << c << ".";
        }
    }

    void split (Samples *ts, unsigned F) {
        ts->resize(size());
        for (unsigned c = 0; c < size(); ++c) {
            auto &in = at(c);
            auto &out = ts->at(c);
            unsigned n = in.size() / F;
            CHECK(n > 0) << "too many folds, not enough input files";
            for (unsigned i = 0; i < n; ++i) {
                out.push_back(in.back());
                in.pop_back();
            }
        }
    }

    void save_list (fs::path const &p) {
        fs::ofstream os(p);
        for (unsigned c = 0; c < size(); ++c) {
            for (auto const &path: at(c)) {
                os << c << '\t' << path.native() << endl;
            }
        }
    }

    void save_dataset (fs::path const &ds_path, unsigned resize, Geometry const &geom) {
        //boost::timer::auto_cpu_timer t;
        vector<Line> lines;
        for (unsigned c = 0; c < size(); ++c) {
            for (auto const &path: at(c)) {
                lines.emplace_back(c, path);
            }
        }
        random_shuffle(lines.begin(), lines.end());

        DataSet dataset(ds_path.native(), geom, 0);
        std::atomic<unsigned> done(0);
#pragma omp parallel for
        for (unsigned i = 0; i < lines.size(); ++i) {
            Record rec;
            string const &path = lines[i].path.native();
            rec.meta.label = lines[i].label;
            cv::Mat image = cv::imread(path, cv::IMREAD_COLOR);
            vector<uint8_t> jpeg;
            if (image.total() == 0) {
                LOG(WARNING) << "fail to load image " << path;
                continue;
            }
            unsigned n = done.fetch_add(1);
            rec.meta.serial = n;
            if (resize) {
                cv::Mat image2;
                cv::resize(image, image2, cv::Size(resize, resize));
                cv::imencode(".jpg", image2, jpeg);
            }
            Json json = Json::object {
                {"path", path},
            };
            string extra = json.dump();
            rec.image_size = jpeg.size();
            rec.image = reinterpret_cast<char const *>(&jpeg[0]);
            rec.extra = &extra[0];
            rec.extra_size = extra.size();
#pragma omp critical
            dataset << rec;
            LOG_IF(INFO, n && ((n % 1000) == 0)) << n << '/' << lines.size() << " images.";
        }
    }

    void resample (int s) {
        CHECK(0) << "not implemented.";
    }
};

size_t KB = 1024;
size_t MB = KB * KB;
size_t GB = MB * KB;

int main (int argc, char *argv[]) {
    //FLAGS_minloglevel=1;
    google::InitGoogleLogging(argv[0]);
    namespace po = boost::program_options; 
    fs::path in_dir;
    fs::path out_dir;
    size_t streams;
    double file_gbs;
    double container_mbs;
    int resize;
    unsigned F;
    int resample;
    int grouping;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("input", po::value(&in_dir), "input directory")
    ("output", po::value(&out_dir), "output directory")
    ("streams,s", po::value(&streams)->default_value(20), "")
    ("file-gbs,f", po::value(&file_gbs)->default_value(4), "")
    ("container-mbs,c", po::value(&container_mbs)->default_value(200), "")
    ("resize,r", po::value(&resize)->default_value(256), "")
    ("shuffle", "")
    ("resample", po::value(&resample)->default_value(0), "")
    ("grouping", po::value(&grouping)->default_value(0), "")
    (",F", po::value(&F)->default_value(10), "")
    ;   
    
    po::positional_options_description p;
    p.add("input", 1);
    p.add("output", 1); 

    po::variables_map vm; 
    po::store(po::command_line_parser(argc, argv).
                     options(desc).positional(p).run(), vm);
    po::notify(vm); 

    if (vm.count("help") 
            || vm.count("input") == 0
            || vm.count("output") == 0) {
        cerr << desc;
        return 1;
    }

    Samples train(in_dir);
    Samples val;
    train.split(&val, F);

    train.save_list(out_dir/fs::path("train.list"));
    val.save_list(out_dir/fs::path("val.list"));

    if (resample != 0) {
        train.resample(resample);
    }

    Geometry geom;
    geom.n_stream = streams;
    geom.file_size = round(file_gbs * GB);
    geom.container_size = round(container_mbs * MB);

    train.save_dataset(out_dir/fs::path("train.pic"), resize, geom);
    val.save_dataset(out_dir/fs::path("val.pic"), resize, geom);

#if 0
    for (unsigned c = 0; c < sdir.size(); ++c) {
        for (auto const &p: sdir[c]) {
            cerr << c << '\t' << p.native() << endl;
        }
    }
    vector<Line> lines;
    {
        Line line;
        line.serial = 0;
        line.label = label;
        LOG(INFO) << "Loaded " << lines.size() << " lines." << endl;
        if (list.size()) {
            ofstream os(list.c_str());
            for (auto const &l: lines) {
                os << l.path << '\t' << l.label << endl;
            }
        }
    }

    if (vm.count("shuffle")) {
        random_shuffle(lines.begin(), lines.end());
    }
#endif
}
