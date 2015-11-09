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
    string in_path;
    size_t batch;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("in", po::value(&in_path), "")
    ("batch", po::value(&batch)->default_value(200000), "")
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

    {
        DataMux mux(in_path);
        for (;;) {
            boost::timer::auto_cpu_timer t;
            for (size_t i = 0; i < batch; ++i) {
                Sample sample; 
                mux.read(&sample);
            }
            cout << batch << " images read." << endl;
        }
    }
}

