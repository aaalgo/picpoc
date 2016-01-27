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

// Configurations
// 0. All categories into a single database
// 1. All categories into separate databases
// 2. Category 0 into a database,
//    all other categories into a single database

// paths under a directory, following symlinks
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

    // move 1/F of the data to a validation set
    void split (Samples *ts, unsigned F, unsigned cap) {
        ts->resize(size());
        for (unsigned c = 0; c < size(); ++c) {
            auto &in = at(c);
            auto &out = ts->at(c);
            unsigned n = in.size() / F;
            CHECK(n >= 1) << "too many folds, not enough input files";
            if (n > cap) n = cap;
            for (unsigned i = 0; i < n; ++i) {
                out.push_back(in.back());
                in.pop_back();
            }
        }
    }

    void append_list (fs::path const &p) {
        fs::ofstream os(p, ios::app);
        for (unsigned c = 0; c < size(); ++c) {
            for (auto const &path: at(c)) {
                os << c << '\t' << path.native() << endl;
            }
        }
    }

    void save_dataset (fs::path const &ds_path, unsigned base, unsigned resize, Geometry const &geom) {
        //boost::timer::auto_cpu_timer t;
        vector<Line> lines;
        for (unsigned c = 0; c < size(); ++c) {
            for (auto const &path: at(c)) {
                lines.emplace_back(base + c, path);
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

    size_t total () const {
        size_t v = 0;
        for (auto const &l: *this) {
            v += l.size();
        }
        return v;
    }
};

size_t KB = 1024;
size_t MB = KB * KB;
size_t GB = MB * KB;

int main (int argc, char *argv[]) {
    FLAGS_minloglevel=1;
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
    int train_batch;
    int val_batch;
    int max_val_per_cat;
    size_t caffe_min_snapshot_images;
    size_t caffe_max_process_images;
    size_t caffe_max_passes;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("input", po::value(&in_dir), "input directory")
    ("output", po::value(&out_dir), "output directory")
    ("streams,s", po::value(&streams)->default_value(4), "")
    ("file-gbs,f", po::value(&file_gbs)->default_value(4), "")
    ("container-mbs,c", po::value(&container_mbs)->default_value(100), "")
    ("resize,r", po::value(&resize)->default_value(256), "")
    ("shuffle", "")
    ("resample", po::value(&resample)->default_value(0), "")
    ("grouping", po::value(&grouping)->default_value(1), "")
    (",F", po::value(&F)->default_value(10), "")
    ("train-load", po::value(&train_batch)->default_value(20), "")
    ("val-load", po::value(&val_batch)->default_value(4), "")
    ("update-caffex-config,U", "")
    ("max-val-per-cat", po::value(&max_val_per_cat)->default_value(200), "")
    ("caffe-max-process-images", po::value(&caffe_max_process_images)->default_value(1000000), "")
    ("caffe-min-snapshot-images", po::value(&caffe_min_snapshot_images)->default_value(500), "")
    ("caffe-max-passes", po::value(&caffe_max_passes)->default_value(20), "")
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

    if (resample != 0) {
        CHECK(0) << "resampling not implemented";
    }

    Samples all(in_dir);

    Geometry geom;
    geom.n_stream = streams;
    geom.file_size = round(file_gbs * GB);
    geom.container_size = round(container_mbs * MB);

    vector<Samples> groups;
    // groups[] == Samples
    // groups[][] = Samples[] = Paths
    // groups[][][] = Samples[][] = Paths[] = fs::path

    if (grouping == 0) {
        // everything into one group/dataset
        groups.push_back(std::move(all));
    }
    else if (grouping == 1) {
        // everything into its own group/dataset
        groups.resize(all.size());
        for (unsigned i = 0; i < groups.size(); ++i) {
            groups[i].push_back(std::move(all[i]));
        }
    }
    else {
        groups.resize(2);
        // category 0 is one group
        groups[0].push_back(std::move(all[0]));
        // everything else is one group
        for (unsigned i = 1; i < all.size(); ++i) {
            groups[1].push_back(std::move(all[i]));
        }
    }

    fs::ofstream train_mux(out_dir/fs::path("train.mux"));
    fs::ofstream val_mux(out_dir/fs::path("val.mux"));
    unsigned base = 0;
    size_t train_min = 1000000;
    size_t val_min = 1000000;
    for (unsigned i = 0; i < groups.size(); ++i) {
        train_mux << "train" << i << "\t0\t" << train_batch << endl;
        val_mux << "val" << i << "\t0\t" << val_batch << endl;
        Samples &train = groups[i];
        Samples val;
        train.split(&val, F, max_val_per_cat);
        train.append_list(out_dir/fs::path("train.list"));
        val.append_list(out_dir/fs::path("val.list"));
        train.save_dataset(out_dir/fs::path((format("train%d")%i).str()), base, resize, geom);
        val.save_dataset(out_dir/fs::path((format("val%d")%i).str()), base, resize, geom);
        base += train.size();
        train_min = std::min(train_min, train.total());
        val_min = std::min(val_min, val.total());
    }
    if (vm.count("update-caffex-config")) {
        fs::path config_path = out_dir / fs::path("config.json");
        CHECK(fs::exists(config_path)) << "Cannot find config.json";
        string text, err;
        {
            fs::ifstream is(config_path);
            is.seekg(0, ios::end);
            CHECK(is);
            text.resize(is.tellg());
            is.seekg(0);
            is.read(&text[0], text.size());
            CHECK(is) << "failed reading caffex config";
        }
        Json json = Json::parse(text, err);
        CHECK(err.empty()) << "json parse error: " << err;
        Json::object fields(json.object_items());
        fields["train_source"] = "train.mux";
        fields["val_source"] = "val.mux";
        int b = json["train_batch"].int_value();
        // l = #it, so it covers the whole dataset once
        int l = (train_min * groups.size() + b - 1) / b;
        if (l < (caffe_min_snapshot_images + b - 1) / b) {
            l = (caffe_min_snapshot_images + b - 1) / b;
        }
        CHECK(l > 0);
        fields["num_output"] = int(base);
        fields["val_interval"] = l;
        fields["snapshot_interval"] = l;
        int max_it = caffe_max_passes * l;
        if (max_it * b > caffe_max_process_images) {
            max_it = caffe_max_process_images / b;
        }
        fields["max_iter"] = max_it;
        b = fields["val_batch"].int_value();
        l = (val_min * groups.size() + b - 1) / b;
        fields["val_batches"] = l;
        Json o_json(fields);
        text = o_json.dump();
        fs::ofstream os(config_path);
        os.write(&text[0], text.size());
        CHECK(os);
    }
}
