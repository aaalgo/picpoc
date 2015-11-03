#pragma once
#include <string>
#include <vector>
#include <iostream>
#include <memory>
#include <boost/assert.hpp>

namespace picpoc {
    using std::string;
    using std::vector;
    using std::istream;
    using std::ostream;
    using std::unique_ptr;

    /// Essential meta data of an image
    struct __attribute__((__packed__)) Meta { 
        int32_t label;  // -1: unknown
        int32_t serial; // serial number, unique within dataset
    };

    /// Image Record
    /**
     */
    struct Record {
        static uint32_t constexpr MAGIC = 0x50494352;   // "PICR"
        static size_t constexpr MAX_IMAGE_SIZE = 33554432;  // 32M
        static size_t constexpr MAX_EXTRA_SIZE = 1048576;   // 1M
    private:
        struct __attribute__((__packed__)) Header { 
            uint32_t magic;         // 4 bytes
            Meta meta;              // 8 bytes
            uint32_t image_size;    // 4 bytes
            uint32_t extra_size;    // 4 bytes

            bool check () const {
                return (magic == MAGIC)
                    && (image_size <= MAX_IMAGE_SIZE)
                    && (extra_size <= MAX_EXTRA_SIZE);
            }
        };
    public:
        Meta meta;
        string image;   // jpeg-encoded image
        string extra;   // extra data in the form of json

        static constexpr size_t align () {
            return alignof(Header);
        }

        size_t storage_size () const;
        // load from stream
        void load (istream &is);
        // save to stream
        void save (ostream &os) const; 
        // load from buffer, return ending address, or NULL on failure
        // buffer must be properly aligned
        char const *load (char const *);    
        // save to buffer, return ending address, or NULL on failure
        // total buffer size is computed with storage_size
        // buffer must be properly aligned
        char *save (char *) const;
    };

    /// Container
    /**
     */
    class Container: vector<Record> {
        static uint32_t constexpr MAGIC = 0x50494343;   // "PICC"
        static size_t constexpr MAX_CONTAINER_SIZE = 10737418240;   // 10G

        struct __attribute__((__packed__)) Header { 
            uint32_t magic;         // 4 bytes
            uint32_t count;
            uint32_t data_crc;
            uint32_t padding;
            bool check () const {
                return (magic == MAGIC);
            }
        };

        size_t storage_size;
    public:
        using vector<Record>::size;
        using vector<Record>::operator[];

        static constexpr size_t align () {
            return alignof(Header);
        }

        Container ();
        void clear ();
        bool add (Record &, size_t max_sz);
        void load (string const &path);
        void save (string const &path) const;
        void unpack (char const *buf, size_t sz);
        void pack (char **buf, size_t *sz) const;
    };

    struct Geometry {
        size_t n_stream;  // number of streams
        size_t file_size;  // maximal file size
        size_t container_size;  // maximal container size
    };

    struct EoS {

    };

    /**
     * A Stream is a file on disk containing multiple containers.
     * containers are read/writen in serial in an async fashion.
     */
    class Stream {
    public:
        struct EoS {};
        virtual ~Stream ();
        virtual void read (Container *); // throws EoS
        virtual void write (Container const &); // throws EoS
    };

    class InputStream: public Stream {
        string root_dir;
        vector<int> fids;
        bool loop;
    public:
        InputStream (string const &, bool loop_);
        virtual ~InputStream ();
        virtual void read (Container *); // throws EoS
        virtual void write (Container const &) {
            BOOST_VERIFY(0);
        }
    };

    class OutputStream: public Stream {
        string root_dir;
        vector<int> fids;
    public:
        OutputStream (string const &, Geometry const &);
        virtual ~OutputStream ();
        virtual void read (Container *) {
            BOOST_VERIFY(0);
        }
        virtual void write (Container const &);
    };

    class DataSet {
        // meta ...
        // record ...
        enum Mode {
            MODE_READ = 1,
            MODE_WRITE = 2
        };

        Mode mode;
        Geometry geometry;
        unsigned next;
        struct Sub {
            unique_ptr<Stream> stream;
            Container container;
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

