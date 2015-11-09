#include <atomic>
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/timer/timer.hpp>
#include <boost/program_options.hpp>
#include <boost/format.hpp>
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

Geometry LARGE = {20, 28 * GB / 10, 200 * MB};

struct Line {
    string path;
    int label;
    int serial;
};

int main (int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);

    namespace po = boost::program_options; 
    string root;
    string list_path;
    string out_path;
    size_t streams;
    double file_gbs;
    double container_mbs;
    unsigned threads;
    int resize;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("list", po::value(&list_path), "")
    ("root", po::value(&root), "")
    ("out", po::value(&out_path), "")
    ("streams,s", po::value(&streams)->default_value(20), "")
    ("file-gbs,f", po::value(&file_gbs)->default_value(4), "")
    ("container-mbs,c", po::value(&container_mbs)->default_value(200), "")
    ("threads,t", po::value(&threads)->default_value(0), "")
    ("resize,r", po::value(&resize)->default_value(256), "")
    ("shuffle", "")
    ;   
    
    po::positional_options_description p;
    p.add("root", 1); 
    p.add("list", 1); 
    p.add("out", 1); 

    po::variables_map vm; 
    po::store(po::command_line_parser(argc, argv).
                     options(desc).positional(p).run(), vm);
    po::notify(vm); 

    if (vm.count("help") || !root.size() || !out_path.size()) {
        cerr << desc;
        return 1;
    }
    vector<Line> lines;
    {
        ifstream is(list_path);
        Line line;
        line.serial = 0;
        while (is >> line.path >> line.label) {
            lines.push_back(line);
            ++line.serial;
        }
        LOG(INFO) << "Loaded " << lines.size() << " lines." << endl;
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
        DataSet dataset(out_path, geometry);
        std::atomic<unsigned> done(0);
        fs::path root_path(root);
#pragma omp parallel for
        for (unsigned i = 0; i < lines.size(); ++i) {
            Record rec;
            string const &sub = lines[i].path;
            rec.meta.label = lines[i].label;
            rec.meta.serial = lines[i].serial;
            fs::path path = root_path / sub;
            cv::Mat image = cv::imread(path.string(), cv::IMREAD_COLOR);
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
                {"fname", sub},
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
