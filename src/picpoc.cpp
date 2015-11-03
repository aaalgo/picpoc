#include <cstring>
#include <fstream>
#include <algorithm>
#include <boost/crc.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include "picpoc.h"

namespace picpoc {

    using namespace std;
    using boost::lexical_cast;
    namespace fs = boost::filesystem;

    template<typename T> constexpr
    T const& const_max(T const& a, T const& b) {
        return a > b ? a : b;
    }

    static size_t constexpr ALIGN = const_max(alignof(Record::align()), alignof(Container::align()));

    template <typename T>
    T roundup (T v, uint32_t bs = ALIGN) {
        return T((v + bs - 1) / bs * bs);
    };

    size_t Record::storage_size () const {
        size_t sz = sizeof(Record::Header) + image.size() + extra.size();
        return roundup(sz);
    }

    void Record::load (istream &is) {
        Header header;
        is.read(reinterpret_cast<char *>(&header), sizeof(header));
        if (!header.check()) {
            // TODO: set fail bit
            BOOST_VERIFY(0);
        }
        else {
            meta = header.meta;
            image.resize(header.image_size);
            is.read(&image[0], image.size());
            extra.resize(header.extra_size);
            is.read(&extra[0], extra.size());
        }
    }

    void Record::save (ostream &os) const {
        Header header;
        header.magic = MAGIC;
        header.meta = meta;
        header.image_size = image.size();
        header.extra_size = extra.size();
        BOOST_VERIFY(header.check());
        os.write(reinterpret_cast<char const *>(&header), sizeof(header));
        os.write(&image[0], image.size());
        os.write(&extra[0], extra.size());
    }

    char const *Record::load (char const *buf) {
        char const *start = buf;
        Header header;
        header = *reinterpret_cast<Header const *>(buf);
        buf += sizeof(Header);
        if (!header.check()) return nullptr;
        meta = header.meta;
        image.resize(header.image_size);
        memcpy(&image[0], buf, image.size());
        buf += image.size();
        extra.resize(header.extra_size);
        memcpy(&extra[0], buf, extra.size());
        buf += extra.size();
        return start + storage_size();
    }

    char *Record::save (char *buf) const {
        Header header;
        header.magic = MAGIC;
        header.meta = meta;
        header.image_size = image.size();
        header.extra_size = extra.size();
        BOOST_VERIFY(header.check());
        char *begin = buf;
        char *end = begin + storage_size();
        *reinterpret_cast<Header *>(buf) = header;
        buf += sizeof(Header);
        memcpy(buf, &image[0], image.size());
        buf += image.size();
        memcpy(buf, &extra[0], extra.size());
        buf += extra.size();
        fill(buf, end, char(0));
        return end;
    }

    Container::Container (): storage_size(roundup(sizeof(Header))) {
    }

    void Container::clear () {
        vector<Record>::clear();
        storage_size = roundup(sizeof(Header));
    }

    bool Container::add (Record &r, size_t max_sz) {
        BOOST_VERIFY(storage_size <= max_sz);
        size_t new_sz = storage_size + r.storage_size();
        if (new_sz > max_sz) return false;
        storage_size = new_sz;
        emplace_back(std::move(r));
        return true;
    }

    void Container::load (string const &path) {
        ifstream is(path.c_str(), ios::binary);
        is.seekg(0, ios::end);
        size_t sz = is.tellg();
        string buf;
        buf.resize(sz);
        is.seekg(0, ios::beg);
        is.read(&buf[0], buf.size());
        unpack(&buf[0], buf.size());
    }

    void Container::save (string const &path) const {
        char *buf;
        size_t sz;
        pack(&buf, &sz);
        BOOST_VERIFY(sz == storage_size);
        ofstream os(path.c_str(), ios::binary);
        os.write(buf, sz);
        free(buf);
    }

    void Container::unpack (char const *buf, size_t sz) {
        size_t header_size = roundup(sizeof(Header));
        char const *end = buf + sz;
        Header header;
        BOOST_VERIFY(sz >= sizeof(header));
        header = *reinterpret_cast<Header const *>(buf);
        BOOST_VERIFY(header.check());
        if (header.count == 0) return;

        buf += header_size;
        uint32_t crc = boost::crc<32, 0x04C11DB7, 0xFFFFFFFF, 0xFFFFFFFF, true, true>(reinterpret_cast<void const *>(buf), end - buf);
        // TODO: raise instead of VERIFY
        BOOST_VERIFY(crc == header.data_crc);
        clear();
        storage_size = header_size;
        Record rec;
        for (unsigned i = 0; i < header.count; ++i) {
            BOOST_VERIFY(buf < end);
            buf = rec.load(buf);
            bool r = add(rec, MAX_CONTAINER_SIZE);
            BOOST_VERIFY(r);
        }
    }

    void Container::pack (char **pbuf, size_t *psz) const {
        char *buf;
        int r = posix_memalign(reinterpret_cast<void **>(&buf), ALIGN, storage_size);
        if (r) {
            *pbuf = nullptr;
            *psz = 0;
            return;
        }
        *pbuf = buf;
        *psz = storage_size;
        size_t header_size = roundup(sizeof(Header));
        char *begin = buf;
        char *data_begin = buf + header_size;
        char *end = begin + storage_size;
        // encode
        // we skip data here
        fill(begin, data_begin, char(0));  // ensure we fill all the gaps
        buf = data_begin;
        for (Record const &r: *this) {
            BOOST_VERIFY(buf < end);
            buf = r.save(buf);
        }
        BOOST_VERIFY(buf <= end);

        Header header;
        header.magic = MAGIC;
        header.count = size();
        header.data_crc = boost::crc<32, 0x04C11DB7, 0xFFFFFFFF, 0xFFFFFFFF, true, true>(reinterpret_cast<void const *>(data_begin), end - data_begin);
        *reinterpret_cast<Header *>(begin) = header;
    }

    void list_dir (fs::path const &path, fs::file_type type, vector<int> *entries) {
        if (!fs::is_directory(path)) {
            BOOST_VERIFY(0);
        }
        fs::directory_iterator begin(path), end;
        entries->clear();
        for (auto it = begin; it != end; ++it) {
            if (it->status().type() == type) {
                string const &p = it->path().native();
                try {
                    entries->push_back(lexical_cast<int>(p));
                }
                catch (...) {
                    cerr << "Cannot parse " << path << " " << p << endl;
                }
            }
        }
        sort(entries->begin(), entries->end());
    }

    DataSet::DataSet (string const &dir, Geometry const &geometry_)
        : mode(MODE_WRITE),
        geometry(geometry_),
        next(0)
    {  // write
        fs::path root(dir);
        fs::create_directory(root);
        subs.resize(geometry.n_stream);
        for (unsigned i = 0; i < geometry.n_stream; ++i) {
            fs::path sub = root/lexical_cast<string>(i);
            subs[i].stream = std::make_unique<OutputStream>(sub.native(), geometry);
            subs[i].offset = 0;
        }
        BOOST_VERIFY(subs.size());
    }

    DataSet::DataSet (string const &dir, bool loop)
        : mode(MODE_READ),
        next(0)
    { // read
        fs::path root(dir);
        vector<int> ss;
        list_dir(root, fs::directory_file, &ss);
        subs.resize(subs.size());
        for (unsigned i = 0; i < ss.size(); ++i) {
            cerr << "Found in " << dir << " : " << ss[i];
            fs::path sub = root/lexical_cast<string>(ss[i]);
            subs[i].stream = std::make_unique<InputStream>(sub.native(), loop);
            subs[i].offset = 0;
        }
        BOOST_VERIFY(subs.size());
    }

    void DataSet::read (Record *rec) {
        for (;;) {
            if (subs.empty()) throw EoS();
            try {
                Sub &sub = subs[next];
                if (sub.offset >= sub.container.size()) {
                    // need to load new container
                    sub.stream->read(&sub.container);
                    BOOST_VERIFY(sub.container.size());
                    sub.offset = 0;
                }
                std::swap(*rec, sub.container[sub.offset]);
                ++sub.offset;
                next = (next + 1) % subs.size();
                return;
            }
            catch (EoS const &e) {
                // remove stream from list
                for (unsigned i = next; i + 1 < subs.size(); ++i) {
                    std::swap(subs[i], subs[i+1]);
                }
                subs.pop_back();
            }
        }
    }

    void DataSet::write (Record &rec, Locator *loc) {
        Sub &sub = subs[next];
        next = (next + 1) % subs.size();
        for (;;) {
            if (sub.container.add(rec, geometry.container_size)) {
                return;
            }
            sub.stream->write(sub.container);
            sub.container.clear();
        }
    }

    DataSet::~DataSet () {
        if (mode == MODE_WRITE) {
            for (auto &sub: subs) {
                if (sub.container.size()) {
                    sub.stream->write(sub.container);
                }
            }
        }
    }
}

