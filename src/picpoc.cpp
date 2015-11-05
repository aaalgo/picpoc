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

    IoSched global_io;

    template<typename T> constexpr
    T const& const_max(T const& a, T const& b) {
        return a > b ? a : b;
    }

    size_t Record::storage_size () const {
        size_t sz = sizeof(Record::Header) + image_size + extra_size;
        return roundup(sz, HEADER_ALIGN);
    }

    char const *Record::load (char const *buf) {
        char const *start = buf;
        Header header;
        header = *reinterpret_cast<Header const *>(buf);
        buf += sizeof(Header);
        if (!header.check()) return nullptr;
        meta = header.meta;
        image = buf;
        image_size = header.image_size;
        buf += image_size;
        extra = buf;
        extra_size = header.extra_size;
        buf += extra_size;
        return start + storage_size();
    }

    char *Record::save (char *buf, Record *saved) const {
        *saved = *this;
        saved->image = saved->extra = nullptr;

        Header header;
        header.magic = MAGIC;
        header.meta = meta;
        header.image_size = image_size;
        header.extra_size = extra_size;
        BOOST_VERIFY(header.check());
        char *begin = buf;
        char *end = begin + storage_size();
        *reinterpret_cast<Header *>(buf) = header;
        buf += sizeof(Header);
        memcpy(buf, image, image_size);
        saved->image = buf;
        buf += image_size;
        memcpy(buf, extra, extra_size);
        saved->extra = buf;
        buf += extra_size;
        fill(buf, end, char(0));
        return end;
    }

    Container::Container (size_t sz) {
        size_t header_size = roundup(sizeof(Header));
        BOOST_VERIFY(sz % IO_BLOCK_SIZE == 0);
        BOOST_VERIFY(sz > header_size);

        char *memory;
        int r = posix_memalign(reinterpret_cast<void **>(&memory), IO_BLOCK_SIZE, sz);
        BOOST_VERIFY(r == 0);
        BOOST_VERIFY(memory);
        mem_begin = memory;
        mem_end = memory + sz;
        mem_next = mem_begin + header_size;
    }

    bool is_aligned (char *ptr, size_t align) {
        return (uintptr_t)ptr % align == 0;
    }

    Container::Container (char *memory, size_t sz, size_t extend) {
        size_t header_size = roundup(sizeof(Header));
        BOOST_VERIFY(is_aligned(memory, IO_BLOCK_SIZE));   //TODO: if not aligned, allocate and copy
        BOOST_VERIFY(sz % IO_BLOCK_SIZE == 0);
        BOOST_VERIFY(sz >= header_size);
        if (extend > sz) {
            BOOST_VERIFY(extend % IO_BLOCK_SIZE);
            BOOST_VERIFY(extend > header_size);
            char *m;
            int r = posix_memalign(reinterpret_cast<void **>(&m), IO_BLOCK_SIZE, extend);
            BOOST_VERIFY(r == 0);
            BOOST_VERIFY(m);
            memcpy(m, memory, sz);
            free(memory);
            memory = m;
            sz = extend;
        }
        mem_begin = memory;
        mem_end = memory + sz;
        // unpack
        char *buf = mem_begin;
        Header header;
        header = *reinterpret_cast<Header const *>(buf);
        BOOST_VERIFY(header.check());

        buf += header_size;

        uint32_t crc = boost::crc<32, 0x04C11DB7, 0xFFFFFFFF, 0xFFFFFFFF, true, true>(reinterpret_cast<void const *>(buf), header.data_size);
        // TODO: raise instead of VERIFY
        BOOST_VERIFY(crc == header.data_crc);

        Record rec;
        for (unsigned i = 0; i < header.count; ++i) {
            BOOST_VERIFY(buf < mem_end);
            buf = const_cast<char *>(rec.load(buf));
            push_back(rec);
        }
        mem_next = buf;
    }

    Container::~Container () {
        free(mem_begin);
    }

    bool Container::add (Record &r, size_t max_sz) {
        char *new_next = mem_next + r.storage_size();
        if (new_next >= mem_end) return false;
        Record rec;
        mem_next = r.save(mem_next, &rec);
        push_back(rec);
        BOOST_VERIFY(new_next == mem_next);
        return true;
    }

    void Container::pack (char const**pbuf, size_t *psz) {
        size_t header_size = roundup(sizeof(Header));
        size_t sz = roundup(mem_next - mem_begin, IO_BLOCK_SIZE);

        char *data_begin = mem_begin + header_size;
        char *data_end = mem_next;
        char *pack_end = mem_begin + sz;
        BOOST_VERIFY(pack_end <= mem_end);

        fill(mem_begin, data_begin, char(0));  // ensure we fill all the gaps
        fill(data_end, pack_end, char(0));

        Header header;
        header.magic = MAGIC;
        header.count = size();
        header.data_size = data_end - data_begin;
        header.data_crc = boost::crc<32, 0x04C11DB7, 0xFFFFFFFF, 0xFFFFFFFF, true, true>(reinterpret_cast<void const *>(data_begin), header.data_size);
        *reinterpret_cast<Header *>(mem_begin) = header;

        *pbuf = mem_begin;
        *psz = sz;
    }

    void list_dir (fs::path const &path, fs::file_type type, vector<int> *entries) {
        if (!fs::is_directory(path)) {
            BOOST_VERIFY(0);
        }
        fs::directory_iterator begin(path), end;
        entries->clear();
        for (auto it = begin; it != end; ++it) {
            if (it->status().type() == type) {
                string p = it->path().filename().native();
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

    InputStream::InputStream (string const &path, bool loop_, IoSched *io_)
        : Stream(path, io_),
        loop(loop_)
    {
        list_dir(fs::path(root), fs::regular_file, &subs);
        BOOST_VERIFY(subs.size());
        pending = io->schedule(dev, [this](){this->prefetch();});
    }

    unique_ptr<Container> InputStream::read () {
        pending.wait();
        auto ptr = std::move(container);//make_unique<Container>(next_buf, next_size);
        BOOST_VERIFY(ptr);
        pending = io->schedule(dev, [this]() {this->prefetch();});
        return ptr;
    }

    void InputStream::prefetch () {
        for (unsigned i = 0; i <= subs.size(); ++i) {
            if (!file) {    // open file
                if (index >= subs.size()) {
                    if (loop) {
                        index = 0;
                    }
                    else {
                        throw EoS();
                    }
                }
                fs::path path(fs::path(root) / lexical_cast<string>(subs[index]));
                ++index;
                file = make_unique<DirectFile>(path.native(), MODE_READ);
            }
            BOOST_VERIFY(file);
            try {
                char *buf;
                size_t size;
                file->alloc_read(&buf, &size);
                container = make_unique<Container>(buf, size);
                return;
            }
            catch (EoS const &e) {
                file.reset();
            }
        }
        BOOST_VERIFY(0);
    }

    OutputStream::OutputStream (string const &path, Geometry const &geometry, IoSched *io_)
        : Stream(path, io_),
        file_size(geometry.file_size)
    {
    }

    void OutputStream::write (unique_ptr<Container> &&c) {
        //cerr << "STREAM WRITE" << endl;
        if (pending.valid()) {
            pending.wait();
        }
        container = std::move(c);
        pending = io->schedule(dev, [this]() {this->flush();});
    }

    void OutputStream::flush () {
        //cerr << "STREAM WRITE" << endl;
        char const *buf;
        size_t size;
        container->pack(&buf, &size);
        for (unsigned i = 0; i < 2; ++i) {
            if (!file) {    // open file
                fs::path path(fs::path(root) / lexical_cast<string>(index));
                ++index;
                file = make_unique<DirectFile>(path.native(), MODE_WRITE, file_size);
            }
            try {
                file->write(buf, size);
                container.reset();
                return;
            }
            catch (EoS const &e) {
                file.reset();
            }
        }
        BOOST_VERIFY(0);
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
            fs::create_directory(sub);
            subs[i].stream = make_unique<OutputStream>(sub.native(), geometry);
            subs[i].offset = 0;
            subs[i].container = make_unique<Container>(geometry.container_size);
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
        subs.resize(ss.size());
        for (unsigned i = 0; i < ss.size(); ++i) {
            fs::path sub = root/lexical_cast<string>(ss[i]);
            subs[i].stream = make_unique<InputStream>(sub.native(), loop);
            subs[i].offset = 0;
        }
        BOOST_VERIFY(subs.size());
    }

    void DataSet::read (Record *rec) {
        for (;;) {
            if (subs.empty()) throw EoS();
            try {
                Sub &sub = subs[next];
                if (!sub.container || (sub.offset >= sub.container->size())) {
                    // need to load new container
                    sub.container = sub.stream->read();
                    BOOST_VERIFY(sub.container->size());
                    sub.offset = 0;
                }
                *rec = sub.container->at(sub.offset);
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
            if (sub.container->add(rec, geometry.container_size)) {
                return;
            }
            sub.stream->write(std::move(sub.container));
            sub.container = make_unique<Container>(geometry.container_size);
        }
    }

    DataSet::~DataSet () {
        if (mode == MODE_WRITE) {
            for (auto &sub: subs) {
                if (sub.container->size()) {
                    sub.stream->write(std::move(sub.container));
                }
            }
        }
    }
}

