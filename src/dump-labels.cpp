#include <atomic>
#include <iostream>
#include <boost/timer/timer.hpp>
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <opencv2/opencv.hpp>
#include <json11.hpp>
#include "picpoc.h"

using namespace std;
using namespace picpoc;

int main (int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);

    namespace po = boost::program_options; 
    string synsets_path;
    string in_path;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("in", po::value(&in_path), "")
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

    int flags = 0;
    if (vm.count("rr")) flags |= READ_RR;
    if (vm.count("loop")) flags |= READ_LOOP;

    start_io();
    {
        DataSet dataset(in_path, flags);
        for (;;) {
            Record rec;
            try {
                dataset.read(&rec);
                cout << rec.meta.label << endl;
            }
            catch (EoS const &) {
                break;
            }
        }
    }
    stop_io();
}

