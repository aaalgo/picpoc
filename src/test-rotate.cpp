#include <unordered_map>
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
    BOOST_VERIFY(argc == 3);
    google::InitGoogleLogging(argv[0]);
    DataSet::rotate(argv[1], argv[2]);
    /*
    unordered_map<unsigned, int> x;
    count(argv[1], &x, 1);
    count(argv[2], &x, -1);
    for (auto const &p: x) {
        BOOST_VERIFY(p.second == 0);
    }
    */
}
