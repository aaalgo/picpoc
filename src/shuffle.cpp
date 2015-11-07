#include <iostream>
#include <boost/program_options.hpp>
#include <boost/timer/timer.hpp>
#include "picpoc.h"

using namespace std;
using namespace picpoc;

size_t KB = 1024;
size_t MB = KB * KB;
size_t GB = MB * KB;

int main (int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);

    namespace po = boost::program_options; 
    string in_path;
    string out_path;
    size_t streams;
    double file_gbs;
    double container_mbs;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("in", po::value(&in_path), "")
    ("out", po::value(&out_path), "")
    ("streams,s", po::value(&streams)->default_value(20), "")
    ("file-gbs,f", po::value(&file_gbs)->default_value(4), "")
    ("container-mbs,c", po::value(&container_mbs)->default_value(200), "")
    ("verify", "")
    ;   
    
    po::positional_options_description p;
    p.add("in", 1); 
    p.add("out", 1); 

    po::variables_map vm; 
    po::store(po::command_line_parser(argc, argv).
                     options(desc).positional(p).run(), vm);
    po::notify(vm); 

    if (vm.count("help") || !in_path.size() || !out_path.size()) {
        cerr << desc;
        return 1;
    }

    Geometry geometry;
    geometry.n_stream = streams;
    geometry.file_size = round(file_gbs * GB);
    geometry.container_size = round(container_mbs * MB);

    start_io();
    {
        DataSet from(in_path, READ_RR);
        DataSet to(out_path, geometry, WRITE_SHUFFLE);
        for (;;) {
            Record rec;
            try {
                from >> rec;
                to << rec;
            }
            catch (EoS const &) {
                break;
            }
        }
    }
    if (vm.count("verify")) {
        DataSet::verify_content(in_path, out_path, false);
    }
    stop_io();
}

