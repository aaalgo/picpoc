#include <atomic>
#include <iostream>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/timer/timer.hpp>
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <opencv2/opencv.hpp>
#include <json11.hpp>
#include "picpoc.h"

using namespace std;
using namespace picpoc;
using json11::Json;
namespace fs = boost::filesystem;

int main (int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);

    namespace po = boost::program_options; 
    string synsets_path;
    string in_path;
    size_t batch;
    bool decode;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("in", po::value(&in_path), "")
    ("batch", po::value(&batch)->default_value(200000), "")
    ("decode", "")
    ("no-crc", "")
    ("rr", "")
    ("loop", "")
    ;   
    
    po::positional_options_description p;
    p.add("in", 1); 

    po::variables_map vm; 
    po::store(po::command_line_parser(argc, argv).
                     options(desc).positional(p).run(), vm);
    po::notify(vm); 

    if (vm.count("help") || !in_path.size()) {
        cerr << desc;
        return 1;
    }

    decode = vm.count("decode");

    if (vm.count("no-crc")) {
        Container::check_crc = false;
    }

    int flags = 0;
    if (vm.count("rr")) flags |= READ_RR;
    if (vm.count("loop")) flags |= READ_LOOP;

    start_io();
    {
        DataSet dataset(in_path, flags);
        for (;;) {
            boost::timer::auto_cpu_timer t;
            for (size_t i = 0; i < batch; ++i) {
                Record rec;
                dataset.read(&rec);
                if (decode) {
                    cv::Mat buffer(1, rec.image_size, CV_8U, const_cast<void *>(reinterpret_cast<void const *>(rec.image)));
                    cv::Mat image = cv::imdecode(buffer, cv::IMREAD_COLOR);
                    LOG_IF(WARNING, image.total() == 0) << "failed to decode image";
                }
            }
            cout << batch << " images read." << endl;
        }
    }
    stop_io();
}

