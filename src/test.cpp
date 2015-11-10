#include <iostream>
#define timer timer_for_boost_progress_t
#include <boost/progress.hpp>
#undef timer
#include <boost/timer/timer.hpp>
#include "picpoc.h"

using namespace std;
using namespace picpoc;

size_t KB = 1024;
size_t MB = KB * KB;
size_t GB = MB * KB;

Geometry LARGE = {3, 5 * GB / 10, 20 * MB};

int main (int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);

    unsigned N = 50000;
    int r = system("rm -rf test-dataset");
    BOOST_VERIFY(r == 0);
    Record rec;
    string image;
    string extra;
    image.resize(4189);
    extra.resize(523);
    rec.meta.label = 0;
    rec.meta.serial = 0;
    rec.image = &image[0];
    rec.image_size = image.size();
    rec.extra = &extra[0];
    rec.extra_size = extra.size();
    Container::check_crc = false;
    {
        cerr << "Writing..." << endl;
        boost::timer::auto_cpu_timer t;
        DataSet dataset("test-dataset", LARGE, 0);
        DataSet::Locator loc;
        boost::progress_display progress(N, cerr);
        for (unsigned i = 0; i < N; ++i) {
            rec.meta.label = i / 1000;
            rec.meta.serial = i;
            dataset.write(rec, &loc);
            ++progress;
        }
    }
    for (unsigned repeat = 1; repeat <= 2; ++repeat) {
        cerr << "Repeat " << repeat << endl;
        int flags = (repeat > 1) ? READ_LOOP : 0;
        {
            cerr << "Reading sequentially..." << endl;
            boost::timer::auto_cpu_timer t;
            DataSet dataset("test-dataset", flags);
            boost::progress_display progress(N * repeat, cerr);
            vector<int> count(N, 0);
            for (unsigned r = 0; r < repeat; ++r) {
                for (unsigned i = 0; i < N; ++i) {
                    dataset.read(&rec);
                    ++count[rec.meta.serial];
                    ++progress;
                }
            }
            for (auto const &v: count) {
                CHECK(v == repeat);
            }
        }
        {
            cerr << "Reading with round robin..." << endl;
            boost::timer::auto_cpu_timer t;
            DataSet dataset("test-dataset", flags | READ_RR);
            boost::progress_display progress(N * repeat, cerr);
            for (unsigned r = 0; r < repeat; ++r) {
                for (unsigned i = 0; i < N; ++i) {
                    dataset.read(&rec);
                    if (r == 0) {
                        BOOST_VERIFY(rec.meta.serial == i);
                    }
                    ++progress;
                }
            }
        }
    }
}
