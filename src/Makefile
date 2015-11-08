CC=g++
CXX=g++
CFLAGS += -g -O0
CXXFLAGS += -Wall -Wno-sign-compare -std=c++1y -fopenmp -g -O0 -pthread -msse4.2
LDFLAGS += -static -fopenmp
LDLIBS += -ljson11 -lopencv_features2d -lopencv_imgcodecs -lopencv_imgproc -lopencv_flann -lopencv_core -lopencv_hal -lIlmImf -lippicv -lboost_timer -lboost_chrono -lboost_program_options -lboost_thread -lboost_filesystem -lboost_system -lopenblas -lturbojpeg -ltiff -lpng -ljasper -lwebp -lglog -lgflags -lunwind -llzma -lz -lm -Wl,--whole-archive -lpthread -Wl,--no-whole-archive -lrt -ldl

COMMON = picpoc.o picpoc-io.o picpoc-shuffle.o

PROGS = load-imagenet unpack-imagenet load-caffe test bench-read test-shuffle dump-labels test-rotate

all:	libpicpoc.a $(PROGS)

libpicpoc.a:	$(COMMON)
	ar rvs $@ $^


test:	test.o libpicpoc.a

load-imagenet:	load-imagenet.o libpicpoc.a

load-caffe:	load-caffe.o libpicpoc.a

unpack-imagenet:	unpack-imagenet.o libpicpoc.a

bench-read:	bench-read.o libpicpoc.a

test-shuffle:	test-shuffle.o libpicpoc.a

test-rotate:	test-rotate.o libpicpoc.a

test-rotate-shuffle:	test-rotate-shuffle.o libpicpoc.a

dump-labels:	dump-labels.o libpicpoc.a

shuffle:	shuffle.o libpicpoc.a

clean:
	rm *.o test
