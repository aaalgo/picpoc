CC=g++
CXX=g++
CFLAGS += -g -O0
CXXFLAGS += -Wall -std=c++1y -fopenmp -g -O0 -pthread
LDFLAGS += -static
LDLIBS += -lopencv_features2d -lopencv_imgproc -lopencv_imgcodecs -lopencv_flann -lopencv_core -lopencv_hal -lIlmImf -lippicv -lboost_timer -lboost_chrono -lboost_program_options -lboost_thread -lboost_filesystem -lboost_system -lopenblas -ljpeg -ltiff -lpng -ljasper -lwebp -lglog -lgflags -lunwind -llzma -lz -lm -Wl,--whole-archive -lpthread -Wl,--no-whole-archive -lrt -ldl

PROGS = test 

all:	$(PROGS)

test:	test.o picpoc.o picpoc-io.o

clean:
	rm *.o test
