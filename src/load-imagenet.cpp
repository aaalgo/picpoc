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
using json11::Json;

size_t KB = 1024;
size_t MB = KB * KB;
size_t GB = MB * KB;

Geometry LARGE = {20, 28 * GB / 10, 200 * MB};

int main (int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);

    namespace po = boost::program_options; 
    string root;
    string synsets_path;
    string out_path;
    size_t streams;
    double file_gbs;
    double container_mbs;
    unsigned threads;
    int resize;
    int max;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("synsets", po::value(&synsets_path)->default_value("synsets.txt"), "")
    ("root", po::value(&root), "")
    ("out", po::value(&out_path), "")
    ("streams,s", po::value(&streams)->default_value(20), "")
    ("file-gbs,f", po::value(&file_gbs)->default_value(4), "")
    ("container-mbs,c", po::value(&container_mbs)->default_value(200), "")
    ("threads,t", po::value(&threads)->default_value(0), "")
    ("resize,r", po::value(&resize)->default_value(256), "")
    ("max", po::value(&max)->default_value(0), "")
    ;   
    
    po::positional_options_description p;
    p.add("root", 1); 
    p.add("out", 1); 
    p.add("synsets", 1); 

    po::variables_map vm; 
    po::store(po::command_line_parser(argc, argv).
                     options(desc).positional(p).run(), vm);
    po::notify(vm); 

    if (vm.count("help") || !root.size() || !out_path.size()) {
        cerr << desc;
        return 1;
    }
    vector<string> tar_paths;
    {
        ifstream is(synsets_path);
        string synset;
        while (is >> synset) {
            string path = root + "/" + synset + ".tar";
            CHECK(boost::filesystem::exists(path)) << path;
            tar_paths.push_back(path);
        }
        LOG(INFO) << "Loaded " << tar_paths.size() << " tar paths." << endl;
    }

    Geometry geometry;
    geometry.n_stream = streams;
    geometry.file_size = round(file_gbs * GB);
    geometry.container_size = round(container_mbs * MB);

    start_io();
    {
        boost::timer::auto_cpu_timer t;
        DataSet dataset(out_path, geometry);
        std::atomic<unsigned> serial(0);
        std::atomic<unsigned> done(0);
        char const *extra = "";
#pragma omp parallel for
        for (unsigned i = 0; i < tar_paths.size(); ++i) {
            Record rec;
            rec.meta.label = i;
            rec.extra = extra;
            rec.extra_size = 0;
            Tar tar(tar_paths[i]);
            vector<uint8_t> jpeg;
            Tar::posix_header const *header;
            while (tar.next(&jpeg, &header)) {
                rec.meta.serial = serial.fetch_add(1);
                if (max && rec.meta.serial >= max) break;
                cv::Mat image = cv::imdecode(cv::Mat(jpeg), cv::IMREAD_COLOR);
                if (image.total() == 0) {
                    LOG(WARNING) << "fail to load image of size " << jpeg.size();
                    continue;
                }
                if (resize) {
                    cv::Mat image2;
                    cv::resize(image, image2, cv::Size(resize, resize));
                    cv::imencode(".jpg", image2, jpeg);
                }
                Json json = Json::object {
                    {"fname", header->name},
                    {"ocols", image.cols},    
                    {"orows", image.rows}
                };
                string extra = json.dump();
                rec.image_size = jpeg.size();
                rec.image = reinterpret_cast<char const *>(&jpeg[0]);
                rec.extra = &extra[0];
                rec.extra_size = extra.size();
                unsigned n = rec.meta.serial + 1;
                LOG_IF(INFO, n && ((n % 1000) == 0)) << done << '/' << tar_paths.size() << " tar_paths, " << serial << " images.";
#pragma omp critical
                dataset << rec;
            }
            ++done;
        }
    }
    stop_io();
}
