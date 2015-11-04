#pragma once
#include <string>
#include <vector>
#include <iostream>
#include <memory>
#include <functional>
#include <future>
#include <queue>
#include <mutex>
#include <limits>
#include <atomic>
#include <map>
#include <condition_variable>
#include <boost/assert.hpp>

namespace picpoc {

    using std::string;
    using std::map;
    using std::future;
    using std::atomic;
    using std::packaged_task;
    using std::vector;
    using std::istream;
    using std::ostream;
    using std::queue;
    using std::unique_ptr;
    using std::function;

    /// Essential meta data of an image
    struct __attribute__((__packed__)) Meta { 
        int32_t label;  // -1: unknown
        int32_t serial; // serial number, unique within dataset
    };

    static size_t constexpr HEADER_ALIGN = 16;
    static size_t constexpr IO_BLOCK_SIZE = 512;
    static_assert(IO_BLOCK_SIZE % HEADER_ALIGN == 0, "alignment");

    template <typename T>
    T roundup (T v, uint32_t bs = HEADER_ALIGN) {
        return T((v + bs - 1) / bs * bs);
    };

    /// Image Record
    /**
     */
    struct Record { // a record does not own the memory of image and extra.
        static uint32_t constexpr MAGIC = 0x50494352;   // "PICR"
        static size_t constexpr MAX_IMAGE_SIZE = 33554432;  // 32M
        static size_t constexpr MAX_EXTRA_SIZE = 1048576;   // 1M
    private:
        struct __attribute__((__packed__)) Header { 
            uint32_t magic;         // 4 bytes
            Meta meta;              // ??
            uint32_t image_size;    // 4 bytes
            uint32_t extra_size;    // 4 bytes

            bool check () const {
                return (magic == MAGIC)
                    && (image_size <= MAX_IMAGE_SIZE)
                    && (extra_size <= MAX_EXTRA_SIZE);
            }
        };

        static_assert(HEADER_ALIGN % alignof(Header) == 0, "alignment");
    public:
        Meta meta;
        char const *image;
        size_t image_size;
        char const *extra;
        size_t extra_size;

        size_t storage_size () const;

        char const *load (char const *);    
        // save to buffer, return ending address, or NULL on failure
        // total buffer size is computed with storage_size
        // buffer must be properly aligned
        char *save (char *, Record *) const;
    };

    /// Container
    /**
     * A container is a chunk of memory backing a bunch of records.
     */
    class Container: vector<Record> {
        static uint32_t constexpr MAGIC = 0x50494343;   // "PICC"
        static size_t constexpr MAX_CONTAINER_SIZE = 10737418240;   // 10G

        struct __attribute__((__packed__)) Header { 
            uint32_t magic;         // 4 bytes
            uint32_t count;
            uint32_t data_size;
            uint32_t data_crc;
            bool check () const {
                return (magic == MAGIC);
            }
        };

        static_assert(HEADER_ALIGN % alignof(Header) == 0, "alignment");

        char *mem_begin;
        char *mem_next;
        char *mem_end;
    public:
        using vector<Record>::size;
        using vector<Record>::operator[];
        using vector<Record>::at;

        size_t packed_size () const {
            return roundup(mem_next - mem_begin, IO_BLOCK_SIZE);
        }

        // construct a new, empty container
        Container (size_t sz);   // from scratch

        // construct a container from existing memory, loading records already in memory
        // containers takes ownership of memory
        Container (char *memory, size_t sz, size_t extend = 0);

        ~Container ();

        bool add (Record &, size_t max_sz);

        void pack (char const**buf, size_t *sz) const;

        Container (Container const &) = delete;
        Container& operator= (Container const &) = delete;
    };

    struct Geometry {
        size_t n_stream;  // number of streams
        size_t file_size;  // maximal file size
        size_t container_size;  // maximal container size
    };

    struct EoS {
    };

    enum IoMode {
        MODE_READ = 1,
        MODE_WRITE = 2
    };

    class IoSched {
        class Device {
            std::mutex mutex;
            std::condition_variable cond;
            queue<packaged_task<void()>> tasks;
        public:
            Device () {
            }

            Device (Device const &) {
                BOOST_VERIFY(tasks.empty());
            }

            Device& operator= (Device const &) = delete;

            future<void> schedule (function<void()> &&fun) {
                std::unique_lock<std::mutex> lock(mutex);
                packaged_task<void()> task(fun);
                future<void> r = task.get_future();
                tasks.push(std::move(task));
                cond.notify_all();
                return r;
            }
            // return true if one is processed
            // return false if nothing is processed
            bool try_process_one () {
                std::unique_lock<std::mutex> lock(mutex);
                if (tasks.empty()) {
                    cond.wait(lock);
                }
                if (tasks.empty()) return false;
                auto task = std::move(tasks.front());
                tasks.pop();
                task();
                return true;
            }

            unsigned capacity () {
                return 1;
            }

            void notify () {
                cond.notify_all();
            }
        };

        map<unsigned, unsigned> lookup;
        vector<Device> devices;
        vector<std::thread> threads;

        bool busy;              // true after start,
                                // false after stop
                                // workers should stop as soon as possible
                                // when busy becomes false
        void work (Device &dev) { // work on device with ID
            while (busy) {
                dev.try_process_one();
            }
        }
    public:
        IoSched ();

        ~IoSched () {
            BOOST_VERIFY(!busy);
            BOOST_VERIFY(threads.empty());
        }

        int identify (string const &path);

        std::future<void> schedule (unsigned d, function<void()> &&task) {
            BOOST_VERIFY(d < devices.size());
            return devices[d].schedule(std::move(task));
        }

        void start () {
            BOOST_VERIFY(!busy);
            BOOST_VERIFY(threads.empty());
            busy = true;
            for (auto &dev: devices) {
                for (unsigned i = 0; i < dev.capacity(); ++i) {
                    threads.emplace_back([this, &dev](){this->work(dev);});
                }
            }
        }
        void stop () {
            BOOST_VERIFY(busy);
            BOOST_VERIFY(threads.size());
            busy = false;
            for (auto &dev: devices) {
                dev.notify();
            }
            for (auto &th: threads) {
                th.join();
            }
            threads.clear();
        }
        IoSched (IoSched const &) = delete;
        IoSched& operator= (IoSched const &) = delete;
    };

    extern IoSched global_io;

    void start_io () {
        global_io.start();
    }
    void stop_io () {
        global_io.stop();
    }


    // file structure
    // --------------
    // directory    -- directory operations are not scheduled, 4K
    // container
    // container
    // container
    // ... ...
    // --------------
    // directory format
    // 
    class DirectFile {
        static uint32_t constexpr MAGIC = 0x50494346;
        static uint32_t constexpr VERSION = 1;
        static size_t constexpr DIRECTORY_STORAGE_SIZE = 4096; 
        static size_t constexpr MAX_DIRECTORY = 255;

        IoMode mode;
        size_t max_size;

        int fd;
        size_t index; 

        class Directory: public vector<uint64_t> {
            struct __attribute__((__packed__)) Header {
                uint32_t magic;
                uint32_t version;
                uint32_t entries;
                uint32_t padding;
            };
            static_assert(HEADER_ALIGN % alignof(Header) == 0, "alignment");
            static_assert(sizeof(Header) + MAX_DIRECTORY * sizeof(uint64_t) <= DIRECTORY_STORAGE_SIZE, "capacity");
        public:
            void read (int fd);
            void write (int fd) const;

            std::pair<uint64_t, uint64_t> range (size_t idx) const {
                if (idx > 0) {
                    return std::make_pair(at(idx-1), at(idx));
                }
                else {  // first container starts after the directory
                    return std::make_pair(DIRECTORY_STORAGE_SIZE, at(idx));
                }
            }

            // append the entry to directory
            // return the offset address the data should be written to
            // check size constraints
            size_t append (size_t sz, size_t limit) {
                BOOST_VERIFY(sz % IO_BLOCK_SIZE == 0);
                if (size() >= MAX_DIRECTORY) throw EoS();
                // allow at least one entry, even if that entry exceeds limit
                if (size() && (back() + sz - DIRECTORY_STORAGE_SIZE >= limit)) throw EoS();
                size_t offset =  DIRECTORY_STORAGE_SIZE;
                if (size()) offset = back();
                push_back(offset + sz);
                return offset;
            }
        };

        Directory dir;

        void open_read (string const &path);
        void open_write  (string const &path);
    public:

        DirectFile (string const &path, IoMode, size_t max_size_ = std::numeric_limits<size_t>::max());
        ~DirectFile ();

        void alloc_read (char **, size_t *);
        void write (char const *, size_t);

        static bool sanity_check (string const &path);
        static void ping (string const &path);
    };

    /**
     * A Stream is a file on disk containing multiple containers.
     * containers are read/writen in serial in an async fashion.
     */
    class Stream {
    protected:
        string root;
        IoSched *io;
        int dev;
        unique_ptr<DirectFile> file;
        future<void> pending;
        unsigned index;
    public:

        Stream (string const &root_, IoSched *io_)
            : root(root_),
            io(io_),
            dev(io ? io->identify(root): 0), 
            index(0)
        {
        }

        virtual ~Stream () {
            if (pending.valid()) pending.wait();
        }

        virtual unique_ptr<Container> read (); // throws EoS
        virtual void write (unique_ptr<Container>);
    };

    class InputStream: public Stream {
        bool loop;
        vector<int> subs;

        unique_ptr<Container> next;
        void prefetch ();
    public:
        InputStream (string const &, bool loop_, IoSched *io_ = &global_io);
        virtual unique_ptr<Container> read (); // throws EoS
        virtual void write (unique_ptr<Container>) {
            BOOST_VERIFY(0);
        }
    };

    class OutputStream: public Stream {
        size_t file_size;
        unique_ptr<Container> prev;
        void flush ();
    public:
        OutputStream (string const &, Geometry const &, IoSched *io_ = &global_io);
        virtual unique_ptr<Container> read () {
            BOOST_VERIFY(0);
        }
        virtual void write (unique_ptr<Container>) ;
    };

    class DataSet {
        // meta ...
        // record ...
        IoMode mode;
        Geometry geometry;
        unsigned next;
        struct Sub {
            unique_ptr<Stream> stream;
            unique_ptr<Container> container;
            unsigned offset;    // only used for reading
        };
        vector<Sub> subs;
    public:
        struct Locator {
            uint32_t sid; // stream id
            uint32_t fid; // file within stream
            uint32_t cid; // container within file
            uint32_t off; // offset within container
        };

        // create an output dataset
        DataSet (string const &, Geometry const &);

        // read existing dataset
        DataSet (string const &, bool loop);

        ~DataSet ();

        void read (Record *);
        void write (Record &, Locator *loc);

        void operator >> (Record &rec) {
            read(&rec);
        }

        void operator << (Record &r) {
            Locator dummy;
            write(r, &dummy);
        }

        static void rotate (string const &input_dir, string const &output_dir);
        static void sample (string const &dir, vector<Locator> *);
    };
}

