#include <iostream>
#include <boost/timer/timer.hpp>
#include <boost/program_options.hpp>
#define CPU_ONLY 1
#include <caffe/util/db.hpp>

using namespace std;

int main (int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);

    namespace po = boost::program_options; 
    string backend;
    string in_path;
    size_t batch;

    po::options_description desc("Allowed options");
    desc.add_options()
    ("help,h", "produce help message.")
    ("in", po::value(&in_path), "")
    ("batch", po::value(&batch)->default_value(200000), "")
    ("backend", po::value(&backend)->default_value("lmdb"), "")
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

    caffe::db::DB *db = caffe::db::GetDB("lmdb");
    CHECK_NOTNULL(db);
    db->Open(in_path, caffe::db::READ);
    auto cursor = db->NewCursor();
    cursor->SeekToFirst();
    for (;;) {
        boost::timer::auto_cpu_timer t;
        for (size_t i = 0; i < batch; ++i) {
            BOOST_VERIFY(cursor->valid());
            BOOST_VERIFY(cursor->value().size());
            cursor->Next();
        }
        cout << batch << " records read." << endl;
    }
    delete db;
}

