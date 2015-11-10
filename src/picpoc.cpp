#include <cstring>
#include <fstream>
#include <algorithm>
#include <boost/crc.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <opencv2/imgcodecs.hpp>
#include "picpoc.h"

namespace picpoc {

    using boost::lexical_cast;
    namespace fs = boost::filesystem;

    IoSched *global_io = nullptr;
    atomic<int> global_io_ref(0);

    GlobalIoUser::GlobalIoUser ()
    {
        int v = global_io_ref.fetch_add(1);
        CHECK(v >= 0);
        if (v == 0) {
            CHECK(global_io == nullptr);
            global_io = new IoSched();
            CHECK_NOTNULL(global_io);
            global_io->start();
        }
    }

    IoSched *GlobalIoUser::io () const {
        return global_io;
    }

    GlobalIoUser::~GlobalIoUser () {
        int v = global_io_ref.fetch_add(-1);
        CHECK(v >= 1);
        if (v == 1) {
            global_io->stop();
            delete global_io;
            global_io = nullptr;
        }
    }

    template<typename T> constexpr
    T const& const_max(T const& a, T const& b) {
        return a > b ? a : b;
    }

    size_t Record::storage_size () const {
        size_t sz = sizeof(Record::Header) + image_size + extra_size;
        return round_up(sz, HEADER_ALIGN);
    }
#ifdef USE_BOOST_CRC
    uint32_t crc32 (char const *buf, size_t sz) {
        return boost::crc_optimal<32, 0x1EDC6F41, 0, 0, true, true>(reinterpret_cast<void const *>(buf), sz);
    }
#else
    uint32_t crc32 (char const *buf, size_t sz) {
        CHECK(sz % 4 == 0);
        uint32_t crc = 0x0;
        uint32_t const *begin = reinterpret_cast<uint32_t const *>(buf);
        sz /= 4;
        uint32_t const *end = begin + sz;
        for (auto ptr = begin; ptr < end; ++ptr) {
            crc = __builtin_ia32_crc32si(crc, *ptr);
        }
        return crc;
    }
#endif

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
        CHECK(header.check());
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
        std::fill(buf, end, char(0));
        return end;
    }

    bool Container::check_crc = false;

    Container::Container (size_t sz) {
        size_t header_size = round_up(sizeof(Header));
        CHECK_EQ(sz % IO_BLOCK_SIZE, 0);
        CHECK(sz > header_size);

        char *memory;
        int r = posix_memalign(reinterpret_cast<void **>(&memory), IO_BLOCK_SIZE, sz);
        CHECK_EQ(r, 0);
        CHECK_NOTNULL(memory);
        mem_begin = memory;
        mem_end = memory + sz;
        mem_next = mem_begin + header_size;
    }

    bool is_aligned (char *ptr, size_t align) {
        return (uintptr_t)ptr % align == 0;
    }

    Container::Container (char *memory, size_t sz, size_t extend) {
        size_t header_size = round_up(sizeof(Header));
        CHECK(is_aligned(memory, IO_BLOCK_SIZE));   //TODO: if not aligned, allocate and copy
        CHECK_EQ(sz % IO_BLOCK_SIZE, 0);
        CHECK(sz >= header_size);
        if (extend > sz) {
            CHECK(extend % IO_BLOCK_SIZE);
            CHECK(extend > header_size);
            char *m;
            int r = posix_memalign(reinterpret_cast<void **>(&m), IO_BLOCK_SIZE, extend);
            CHECK_EQ(r, 0);
            CHECK_NOTNULL(m);
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
        CHECK(header.check());

        buf += header_size;

        if (check_crc) {
            uint32_t crc = crc32(buf, header.data_size);
            // TODO: raise instead of VERIFY
            CHECK_EQ(crc, header.data_crc);
        }

        Record rec;
        for (unsigned i = 0; i < header.count; ++i) {
            CHECK(buf < mem_end);
            buf = const_cast<char *>(rec.load(buf));
            push_back(rec);
        }
        mem_next = buf;
    }

    Container::~Container () {
        if (mem_begin) {
            free(mem_begin);
        }
    }

    bool Container::add (Record &r) {
        char *new_next = mem_next + r.storage_size();
        if (new_next >= mem_end) return false;
        Record rec;
        mem_next = r.save(mem_next, &rec);
        push_back(rec);
        CHECK_EQ(new_next, mem_next);
        return true;
    }

    void Container::pack (char **pbuf, size_t *psz) {
        CHECK_NOTNULL(mem_begin);
        size_t header_size = round_up(sizeof(Header));
        size_t sz = round_up(mem_next - mem_begin, IO_BLOCK_SIZE);

        char *data_begin = mem_begin + header_size;
        char *data_end = mem_next;
        char *pack_end = mem_begin + sz;
        CHECK(pack_end <= mem_end);

        std::fill(mem_begin, data_begin, char(0));  // ensure we fill all the gaps
        std::fill(data_end, pack_end, char(0));

        Header header;
        header.magic = MAGIC;
        header.count = size();
        header.data_size = data_end - data_begin;
        header.data_crc = crc32(data_begin, header.data_size);
        *reinterpret_cast<Header *>(mem_begin) = header;

        *pbuf = mem_begin;
        *psz = sz;
        mem_begin = mem_next = mem_end = nullptr;
        clear();
    }

    void list_dir (fs::path const &path, fs::file_type type, vector<int> *entries) {
        if (!fs::is_directory(path)) {
            CHECK(0);
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
                    LOG(WARNING) << "Cannot parse " << path << " " << p;
                }
            }
        }
        sort(entries->begin(), entries->end());
    }

    void Stream::ping (string const &path, Info *info) {
        list_dir(fs::path(path), fs::regular_file, &info->subs);
    }

    InputStream::InputStream (string const &path, bool loop_, bool prefetch_, IoSched *io_)
        : Stream(path, io_),
        loop(loop_)
    {
        list_dir(fs::path(root), fs::regular_file, &subs);
        CHECK(subs.size());
        if (prefetch_) {
            pending = io->schedule(dev, [this](){this->prefetch();});
        }
    }

    unique_ptr<Container> InputStream::read () {
        if (!pending.valid()) {
            pending = io->schedule(dev, [this](){this->prefetch();});
        }
        pending.wait();
        if (!buf) {
            pending = future<void>();
            throw EoS();
        }
        CHECK(buf_size);
        char *tmp_buf = buf;
        size_t tmp_buf_size = buf_size;
        buf = nullptr;
        buf_size = 0;
        // prefetch as soon as possible
        pending = io->schedule(dev, [this]() {this->prefetch();});
        return make_unique<Container>(tmp_buf, tmp_buf_size);
    }

    void InputStream::prefetch () {
        for (unsigned i = 0; i <= subs.size(); ++i) {
            if (!file) {    // open file
                if (index >= subs.size()) {
                    index = 0;
                    if (!loop) {
                        throw EoS();
                    }
                }
                fs::path path(fs::path(root) / lexical_cast<string>(subs[index]));
                ++index;
                file = make_unique<DirectFile>(path.native(), MODE_READ);
            }
            CHECK(file);
            try {
                CHECK(buf == nullptr);
                CHECK_EQ(buf_size, 0);
                file->alloc_read(&buf, &buf_size);
                return;
            }
            catch (EoS const &e) {
                file.reset();
            }
        }
        CHECK(0);
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
        CHECK(buf == nullptr);
        CHECK_EQ(buf_size, 0);
        unique_ptr<Container> container = std::move(c);
        container->pack(&buf, &buf_size);
        CHECK_NOTNULL(buf);
        CHECK(buf_size);
        pending = io->schedule(dev, [this]() {this->flush();});
    }

    void OutputStream::flush () {
        //cerr << "STREAM WRITE" << endl;
        for (unsigned i = 0; i < 2; ++i) {
            if (!file) {    // open file
                fs::path path(fs::path(root) / lexical_cast<string>(index));
                ++index;
                file = make_unique<DirectFile>(path.native(), MODE_WRITE, file_size);
            }
            try {
                file->write_free(buf, buf_size);
                buf = nullptr;
                buf_size = 0;
                return;
            }
            catch (EoS const &e) {
                file.reset();
            }
        }
        CHECK(0);
    }

    DataSet::DataSet (string const &dir, Geometry const &geometry_, int flags_)
        : mode(MODE_WRITE),
        flags(flags_),
        geometry(geometry_),
        next(0)
    {  // write
        fs::path root(dir);
        fs::create_directory(root);
        subs.resize(geometry.n_stream);
        write_index.resize(geometry.n_stream);
        for (unsigned i = 0; i < geometry.n_stream; ++i) {
            write_index[i] = i;
            fs::path sub = root/lexical_cast<string>(i);
            fs::create_directory(sub);
            subs[i].stream = make_unique<OutputStream>(sub.native(), geometry, io());
            subs[i].offset = 0;
            subs[i].container = make_unique<Container>(geometry.container_size);
        }
        CHECK(subs.size());
    }

    DataSet::DataSet (string const &dir, int flags_)
        : mode(MODE_READ),
        flags(flags_),
        next(0)
    { // read
        fs::path root(dir);
        vector<int> ss;
        list_dir(root, fs::directory_file, &ss);
        subs.resize(ss.size());
        for (unsigned i = 0; i < ss.size(); ++i) {
            fs::path sub = root/lexical_cast<string>(ss[i]);
            subs[i].stream = make_unique<InputStream>(sub.native(), (flags & READ_LOOP) && (flags & READ_RR), flags & READ_RR, io());
            subs[i].offset = 0;
        }
        CHECK(subs.size());
    }

    void DataSet::read (Record *rec) {
        for (;;) {
            if (subs.empty()) throw EoS();
            try {
                if (next >= subs.size()) {
                    next = 0;
                }
                Sub &sub = subs[next];
                if (!sub.container || (sub.offset >= sub.container->size())) {
                    // need to load new container
                    sub.container = sub.stream->read();
                    CHECK(sub.container->size());
                    sub.offset = 0;
                }
                *rec = sub.container->at(sub.offset);
                ++sub.offset;
                if (flags & READ_RR) {
                    next = next + 1;
                }
                return;
            }
            catch (EoS const &e) {
                // remove stream from list
                if (flags & READ_LOOP) {
                    next = next + 1;
                }
                else {  // remove that stream
                    for (unsigned i = next; i + 1 < subs.size(); ++i) {
                        std::swap(subs[i], subs[i+1]);
                    }
                    subs.pop_back();
                }
            }
        }
    }

    void DataSet::write (Record &rec, Locator *loc) {
        if (next >= subs.size()) {
            next = 0;
            if (flags & WRITE_SHUFFLE) {
                random_shuffle(write_index.begin(), write_index.end());
            }
        }
        Sub &sub = subs[write_index[next++]];
        for (;;) {
            if (sub.container->add(rec)) {
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

    void DataSet::ping (string const &path, Info *info) {
        list_dir(fs::path(path), fs::directory_file, &info->subs);
    }

    DataMux::DataMux (string const &config)
        : index(0)
    {
        {
            std::ifstream is(config.c_str());
            Source src;
            while (is >> src.path >> src.label_base >> src.batch_size) {
                sources.push_back(std::move(src));
            }
        }
        unsigned total = 0;
        for (auto &src: sources) {
            src.dataset = make_unique<DataSet>(src.path, READ_LOOP);
            total += src.batch_size;
        }
        CHECK(total > 0);
        batch.resize(total);
        batch_prefetch.resize(total);
        index = batch.size();
        pending = io()->schedule(io()->CPU(), [this](){this->prefetch();});
    }

    void DataMux::wait_data () {
        if (index >= batch.size()) {
            if (!pending.valid()) {
                pending = io()->schedule(io()->CPU(), [this](){this->prefetch();});
            }
            pending.wait();
            batch.swap(batch_prefetch);
            index = 0;
            pending = io()->schedule(io()->CPU(), [this](){this->prefetch();});
        }
    }

    void DataMux::prefetch () {
        Record rec;
        unsigned off = 0;
        for (auto &src: sources) {
            for (unsigned i = 0; i < src.batch_size; ++i) {
                for (;;) {
                    src.dataset->read(&rec);
                    batch_prefetch[off].label = rec.meta.label;
                    cv::Mat buffer(1, rec.image_size, CV_8U, const_cast<void *>(reinterpret_cast<void const *>(rec.image)));
                    batch_prefetch[off].image = cv::imdecode(buffer, cv::IMREAD_COLOR);
                    if (batch_prefetch[off].image.total()) {
                        ++off; break;
                    }
                    LOG(WARNING) << "Fail to decode image.";
                }
            }
        }
        CHECK(off == batch_prefetch.size());
        std::random_shuffle(batch_prefetch.begin(), batch_prefetch.end());
    }
}

