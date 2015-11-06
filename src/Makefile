CC=g++
CXX=g++
CFLAGS += -g -O0
CXXFLAGS += -Wall -Wno-sign-compare -std=c++1y -fopenmp -g -O0 -pthread
LDFLAGS += -static -fopenmp
LDLIBS += -ljson11 -lopencv_features2d -lopencv_imgcodecs -lopencv_imgproc -lopencv_flann -lopencv_core -lopencv_hal -lIlmImf -lippicv -lboost_timer -lboost_chrono -lboost_program_options -lboost_thread -lboost_filesystem -lboost_system -lopenblas -lturbojpeg -ltiff -lpng -ljasper -lwebp -lglog -lgflags -lunwind -llzma -lz -lm -Wl,--whole-archive -lpthread -Wl,--no-whole-archive -lrt -ldl

COMMON = picpoc.o picpoc-io.o picpoc-shuffle.o

PROGS = load-imagenet unpack-imagenet load-caffe test bench-read test-shuffle dump-labels test-rotate

all:	$(PROGS)

test:	test.o $(COMMON)

load-imagenet:	load-imagenet.o $(COMMON)

load-caffe:	load-caffe.o $(COMMON)

unpack-imagenet:	unpack-imagenet.o $(COMMON)

bench-read:	bench-read.o $(COMMON)

test-shuffle:	test-shuffle.o $(COMMON)

test-rotate:	test-rotate.o $(COMMON)

dump-labels:	dump-labels.o $(COMMON)

clean:
	rm *.o test
