#include <atomic>
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/timer/timer.hpp>
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/filesystem.hpp>
#include <opencv2/opencv.hpp>
#include <json11.hpp>
#include "picpoc.h"
#include "tar.h"

using namespace std;
using namespace picpoc;
namespace fs = boost::filesystem;
using json11::Json;

size_t KB = 1024;
size_t MB = KB * KB;
size_t GB = MB * KB;

struct Line {
    string path;
    int label;
    int serial;
};

int main (int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    namespace po = boost::program_options; 
    vector<string> dirs;
    string out_path;
    string list;
    size_t streams;
    double file_gbs;
    double container_mbs;
    int resize;
    int label;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("dir", po::value(&dirs), "")
    ("out", po::value(&out_path), "")
    ("streams,s", po::value(&streams)->default_value(20), "")
    ("file-gbs,f", po::value(&file_gbs)->default_value(4), "")
    ("container-mbs,c", po::value(&container_mbs)->default_value(200), "")
    ("resize,r", po::value(&resize)->default_value(256), "")
    ("label,l", po::value(&label)->default_value(0), "")
    ("shuffle", "")
    ("list", po::value(&list), "")
    ;   
    
    po::positional_options_description p;
    p.add("out", 1); 
    p.add("dir", -1); 

    po::variables_map vm; 
    po::store(po::command_line_parser(argc, argv).
                     options(desc).positional(p).run(), vm);
    po::notify(vm); 

    if (vm.count("help") || !out_path.size()) {
        cerr << desc;
        return 1;
    }
    vector<Line> lines;
    {
        Line line;
        line.serial = 0;
        line.label = label;
        for (string const &dir: dirs) {
            fs::recursive_directory_iterator it(fs::path(dir), fs::symlink_option::recurse), end;
            for (; it != end; ++it) {
                if (it->status().type() == fs::regular_file) {
                    line.path = it->path().string();
                    lines.push_back(line);
                    ++line.serial;
                }
            }
        }
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

    Geometry geometry;
    geometry.n_stream = streams;
    geometry.file_size = round(file_gbs * GB);
    geometry.container_size = round(container_mbs * MB);

    {
        boost::timer::auto_cpu_timer t;
        DataSet dataset(out_path, geometry, 0);
        std::atomic<unsigned> done(0);
#pragma omp parallel for
        for (unsigned i = 0; i < lines.size(); ++i) {
            Record rec;
            string const &path = lines[i].path;
            rec.meta.label = lines[i].label;
            rec.meta.serial = lines[i].serial;
            cv::Mat image = cv::imread(path, cv::IMREAD_COLOR);
            vector<uint8_t> jpeg;
            if (image.total() == 0) {
                LOG(WARNING) << "fail to load image " << path;
                continue;
            }
            if (resize) {
                cv::Mat image2;
                cv::resize(image, image2, cv::Size(resize, resize));
                cv::imencode(".jpg", image2, jpeg);
            }
            Json json = Json::object {
                {"fname", path},
                {"ocols", image.cols},    
                {"orows", image.rows}
            };
            string extra = json.dump();
            rec.image_size = jpeg.size();
            rec.image = reinterpret_cast<char const *>(&jpeg[0]);
            rec.extra = &extra[0];
            rec.extra_size = extra.size();
#pragma omp critical
            dataset << rec;
            unsigned n = done.fetch_add(1);
            LOG_IF(INFO, n && ((n % 1000) == 0)) << n << '/' << lines.size() << " images.";
        }
    }
}
