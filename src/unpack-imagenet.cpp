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
#include "tar.h"

using namespace std;
using namespace picpoc;
using json11::Json;
namespace fs = boost::filesystem;

int main (int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);

    namespace po = boost::program_options; 
    string synsets_path;
    string in_path;
    string out_path;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("synsets", po::value(&synsets_path)->default_value("synsets.txt"), "")
    ("in", po::value(&in_path), "")
    ("out", po::value(&out_path), "")
    ;   
    
    po::positional_options_description p;
    p.add("in", 1); 
    p.add("out", 1); 
    p.add("synsets", 1); 

    po::variables_map vm; 
    po::store(po::command_line_parser(argc, argv).
                     options(desc).positional(p).run(), vm);
    po::notify(vm); 

    if (vm.count("help") || !in_path.size() || !out_path.size()) {
        cerr << desc;
        return 1;
    }
    vector<string> synsets;
    fs::path root(out_path);
    {
        ifstream is(synsets_path);
        string synset;
        while (is >> synset) {
            synsets.push_back(synset);
            fs::path path = root / synset;
            fs::create_directories(path);
        }
    }

    {
        boost::timer::auto_cpu_timer t;
        DataSet dataset(in_path, 0);
        for (;;) {
            try {
                Record rec;
                dataset.read(&rec);
                CHECK(rec.meta.label < synsets.size()) << "invalid label " << rec.meta.label;
                string err;
                Json json = Json::parse(string(rec.extra, rec.extra + rec.extra_size), err);
                //CHECK_NE(json, Json()) << "error parsing json for record " << rec.meta.serial;
                fs::path tmp(json["fname"].string_value());
                fs::path path = root / synsets[rec.meta.label] / tmp.filename();
                fs::ofstream os(path, ios::binary);
                os.write(rec.image, rec.image_size);
            }
            catch (picpoc::EoS const &e) {
                break;
            }
        }
    }
}
