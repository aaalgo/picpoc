#include <linux/types.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <fstream>
#include <algorithm>
#include <boost/crc.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include "picpoc.h"

namespace picpoc {
    using std::ifstream;

    int identify_physical_disk (dev_t dev) {
        return dev /16;
    }

    void DirectFile::Directory::read (int fd) {
        char *buf;
        int r = posix_memalign(reinterpret_cast<void **>(&buf), IO_BLOCK_SIZE, DIRECTORY_STORAGE_SIZE);
        CHECK_EQ(r, 0);
        CHECK_NOTNULL(buf);
        r = pread(fd, buf, DIRECTORY_STORAGE_SIZE, 0);
        CHECK_EQ(r, DIRECTORY_STORAGE_SIZE);
        Header header = *reinterpret_cast<Header *>(buf);
        CHECK_EQ(header.magic, MAGIC);
        CHECK_EQ(header.version, VERSION);
        CHECK(header.entries <= MAX_DIRECTORY);
        resize(header.entries);
        memcpy(&at(0), buf + sizeof(header), size() * sizeof(at(0)));
        free(buf);
    }

    void DirectFile::Directory::write (int fd) const {
        char *buf;
        int r = posix_memalign(reinterpret_cast<void **>(&buf), IO_BLOCK_SIZE, DIRECTORY_STORAGE_SIZE);
        CHECK_EQ(r, 0);
        CHECK_NOTNULL(buf);
        Header header;
        header.magic = MAGIC;
        header.version = VERSION;
        header.entries = size();
        header.padding = 0;
        *reinterpret_cast<Header *>(buf) = header;
        memcpy(buf + sizeof(header), &at(0), size() * sizeof(at(0)));
        ssize_t sz = pwrite(fd, buf, DIRECTORY_STORAGE_SIZE, 0);
        CHECK_EQ(sz, DIRECTORY_STORAGE_SIZE);
        free(buf);
    }

    DirectFile::DirectFile (string const &path, IoMode mode_, size_t max_size_)
        : mode(mode_),
        max_size(max_size_),
        fd(-1),
        index(0)
    {
        if (mode == MODE_READ) {
            open_read(path);
        }
        else if (mode == MODE_WRITE) {
            open_write(path);
        }
        else {
            CHECK(0);
        }
    }

    DirectFile::~DirectFile () {
        if (mode == MODE_WRITE) {
            dir.write(fd);
        }
        close(fd);
    }

    void DirectFile::open_read (string const &path) {
        fd = open(path.c_str(), O_RDONLY | O_DIRECT | O_SYNC);
        CHECK(fd >= 0);
        dir.read(fd);
    }

    void DirectFile::open_write (string const &path) {
        fd = open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_DIRECT | O_SYNC, 0666);
        CHECK(fd >= 0);
    }

    void DirectFile::alloc_read (char **pbuf, size_t *psz) {
        if (index >= dir.size()) throw EoS();
        size_t begin, end;
        std::tie(begin, end) = dir.range(index);
        ++index;
        size_t off = begin;
        size_t sz = end - begin;

        char *buf;
        int r = posix_memalign(reinterpret_cast<void **>(&buf), IO_BLOCK_SIZE, sz);
        if (r) {
            *pbuf = nullptr;
            *psz = 0;
            CHECK(0);
        }
        *pbuf = buf;
        *psz = sz;
        r = pread(fd, buf, sz, off);
        CHECK_EQ(r, sz);
    }

    void DirectFile::write_free (char *buf, size_t sz) {
        size_t off = dir.append(sz, max_size);
        int r = pwrite(fd, buf, sz, off);
        CHECK_EQ(r, sz);
        free(buf);
    }

    IoSched::IoSched (): busy(false) {
        map<int, vector<string>> all;
        ifstream is("/proc/mounts");
        for (;;) {
            string dev, dir, dummy;
            is >> dev >> dir;
            getline(is, dummy);
            if (!is) break;
            if (dev.empty()) continue;
            if (dev[0] == '/') {
                struct stat st;
                int r = stat(dev.c_str(), &st);
                CHECK_EQ(r, 0);
                if (!S_ISBLK(st.st_mode)) continue;
                int disk = identify_physical_disk(st.st_rdev);
                if (disk >= 0) {
                    all[disk].push_back(dir);
                }
            }
        }
        unsigned c = 0;
        all[-1].push_back("CPU");
        for (auto const &p: all) {
            lookup[p.first] = c++;
            Device *ptr = new Device;
            CHECK_NOTNULL(ptr);
            devices.push_back(ptr);
            for (auto const &s: p.second) {
                LOG(INFO) << "Found mount " << s << " on device with ID=" << p.first << '.';
            }
        }
        cpu_dev = lookup[-1];
    }

    int IoSched::identify (string const &path) {
        struct stat st;
        int r = stat(path.c_str(), &st);
        CHECK_EQ(r, 0);
        int disk = identify_physical_disk(st.st_dev);
        auto it = lookup.find(disk);
        CHECK(it != lookup.end());
        return it->second;
    }

    void DirectFile::ping (string const &path, Info *info) {
        DirectFile file(path, MODE_READ);
        info->container_sizes = file.dir;
    }
}

