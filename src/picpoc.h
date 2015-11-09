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
#include <opencv2/core/core.hpp>
#include <glog/logging.h>

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
#if __cplusplus < 201300
    template<typename T, typename... Args>
    std::unique_ptr<T> make_unique(Args&&... args)
    {
        return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
    }
#else
    using std::make_unique;
#endif

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
        static bool check_crc;
        using vector<Record>::size;
        using vector<Record>::operator[];
        using vector<Record>::at;
        using vector<Record>::begin;
        using vector<Record>::end;

        size_t packed_size () const {
            return roundup(mem_next - mem_begin, IO_BLOCK_SIZE);
        }

        // construct a new, empty container
        Container (size_t sz);   // from scratch

        // construct a container from existing memory, loading records already in memory
        // containers takes ownership of memory
        Container (char *memory, size_t sz, size_t extend = 0);

        ~Container ();

        bool add (Record &);

        void pack (char **buf, size_t *sz);

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

            Device& operator= (Device const &) = delete;

            future<void> schedule (function<void()> &&fun) {
                packaged_task<void()> task(fun);
                future<void> r = task.get_future();
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    tasks.push(std::move(task));
                }
                cond.notify_all();
                return r;
            }
            // return true if one is processed
            // return false if nothing is processed
            bool try_process_one () {
                packaged_task<void()> task;
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    if (tasks.empty()) {
                        cond.wait(lock);
                    }
                    if (tasks.empty()) return false;
                    task = std::move(tasks.front());
                    tasks.pop();
                }
                // std::cerr << "IO exec" << std::endl;
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

        map<int, unsigned> lookup;
        vector<Device *> devices;
        vector<std::thread> threads;

        bool busy;              // true after start,
                                // false after stop
                                // workers should stop as soon as possible
                                // when busy becomes false
        void work (Device *dev) { // work on device with ID
            //std::cerr << "working" << std::endl;
            CHECK_NOTNULL(dev);
            while (busy) {
                dev->try_process_one();
            }
        }
    public:
        IoSched ();

        ~IoSched () {
            for (auto d: devices) {
                delete d;
            }
            BOOST_VERIFY(!busy);
            BOOST_VERIFY(threads.empty());
        }

        int identify (string const &path);

        std::future<void> schedule (unsigned d, function<void()> &&task) {
            BOOST_VERIFY(d < devices.size());
            return devices[d]->schedule(std::move(task));
        }

        void start () {
            BOOST_VERIFY(!busy);
            BOOST_VERIFY(threads.empty());
            busy = true;
            for (auto dev: devices) {
                for (unsigned i = 0; i < dev->capacity(); ++i) {
                    threads.emplace_back([this, dev](){this->work(dev);});
                }
            }
        }

        void stop () {
            BOOST_VERIFY(busy);
            BOOST_VERIFY(threads.size());
            busy = false;
            for (auto dev: devices) {
                dev->notify();
            }
            for (auto &th: threads) {
                th.join();
            }
            threads.clear();
        }
        IoSched (IoSched const &) = delete;
        IoSched& operator= (IoSched const &) = delete;
    };

    class GlobalIoUser {
    public:
        GlobalIoUser();
        ~GlobalIoUser();
        IoSched *io () const;
    };

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
            static_assert(sizeof(Header) + MAX_DIRECTORY * sizeof(uint64_t) <= DirectFile::DIRECTORY_STORAGE_SIZE, "capacity");
        public:
            void read (int fd);
            void write (int fd) const;

            std::pair<uint64_t, uint64_t> range (size_t idx) const {
                if (idx > 0) {
                    return std::make_pair(at(idx-1), at(idx));
                }
                else {  // first container starts after the directory
                    return std::make_pair(int64_t(DirectFile::DIRECTORY_STORAGE_SIZE), at(idx));
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
        struct Info {
            vector<uint64_t> container_sizes;
        };


        DirectFile (string const &path, IoMode, size_t max_size_ = std::numeric_limits<size_t>::max());
        ~DirectFile ();

        void alloc_read (char **, size_t *);
        void write_free (char *, size_t);

        void write_free (unique_ptr<Container> &&c) {
            char *buf;
            size_t sz;
            c->pack(&buf, &sz);
            write_free(buf, sz);
        }

        static bool sanity_check (string const &path);
        static void shuffle (string const &in_path, string const &out_path);
        static void load (string const &path, vector<unique_ptr<Container>> *);
        static void ping (string const &path, Info *info);
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
        char *buf;
        size_t buf_size;
        //unique_ptr<Container> container;
        future<void> pending;
        unsigned index;
    public:
        struct Info {
            vector<int> subs;
        };

        Stream (string const &root_, IoSched *io_)
            : root(root_),
            io(io_),
            dev(io ? io->identify(root): 0), 
            buf(nullptr),
            buf_size(0),
            index(0)
        {
        }

        virtual ~Stream () {
            if (pending.valid()) pending.wait();
        }

        virtual unique_ptr<Container> read () = 0; // throws EoS
        virtual void write (unique_ptr<Container> &&) = 0;

        static void ping (string const &path, Info *info);
    };

    class InputStream: public Stream {
        bool loop;
        vector<int> subs;

        void prefetch ();
    public:
        // prefetch only controls prefetching the first container
        // after that, prefetching is always done.
        InputStream (string const &, bool loop_, bool prefetch_, IoSched *io_);
        // read will throw EoS
        // a read operation after EoS will start reading from the beginning
        virtual unique_ptr<Container> read (); // throws EoS
        virtual void write (unique_ptr<Container> &&) {
            BOOST_VERIFY(0);
        }
    };

    class OutputStream: public Stream {
        size_t file_size;
        void flush ();
    public:
        OutputStream (string const &, Geometry const &, IoSched *io_);
        virtual unique_ptr<Container> read () {
            BOOST_VERIFY(0);
            return unique_ptr<Container>();
        }
        virtual void write (unique_ptr<Container> &&) ;
    };

    enum ReadFlags {
        READ_LOOP = 1,
        READ_RR = 2,
        WRITE_SHUFFLE = 4
    };

    class DataSet:GlobalIoUser {
        // meta ...
        // record ...
        IoMode mode;
        int flags;
        Geometry geometry;
        unsigned next;
        struct Sub {
            unique_ptr<Stream> stream;
            unique_ptr<Container> container;
            unsigned offset;    // only used for reading
        };
        vector<Sub> subs;
        vector<unsigned> write_index;
    public:
        struct Locator {
            uint32_t sid; // stream id
            uint32_t fid; // file within stream
            uint32_t cid; // container within file
            uint32_t off; // offset within container
        };

        struct Info {
            vector<int> subs;
        };

        // create an output dataset
        DataSet (string const &, Geometry const &, int flags_ = 0);

        // read existing dataset
        DataSet (string const &, int flags_ = 0);

        ~DataSet ();

        size_t streams () const {
            return subs.size();
        }

        void read (Record *);
        void write (Record &, Locator *loc);

        void operator >> (Record &rec) {
            read(&rec);
        }

        void operator << (Record &r) {
            Locator dummy;
            write(r, &dummy);
        }

        static void rotate (string const &input_dir, string const &output_dir, size_t n_stream = 0);
        static void sample (string const &dir, vector<Locator> *);
        static void ping (string const &path, Info *info);
        static void verify_content (string const &path1, string const &path2, bool io);
    };

    struct Sample {
        int label;
        cv::Mat image;
    };

    class DataMux: GlobalIoUser {
        struct Source {
            string path;
            int label_base;
            size_t batch_size;
            unique_ptr<DataSet> dataset;
        };
        vector<Source> sources;
        vector<Sample> batch;
        vector<Sample> batch_prefetch;
        unsigned index;
        future<void> pending;
        void prefetch ();
        void wait_data ();
    public:
        /** Config format:
         * path label_base batch_size
         * path label_base batch_size
         * ...
         */
        DataMux (string const &conig);
        void read (Sample *rec) {
            wait_data();
            *rec = batch[index++];
        }
        void peek (Sample *rec) {
            wait_data();
            *rec = batch[index];
        }
        void operator >> (Sample &rec) {
            read(&rec);
        }
    };
}

