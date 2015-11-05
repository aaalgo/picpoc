CC=g++
CFLAGS += -O3 -g 
CXXFLAGS += -Wall -std=c++1y -O3 -fopenmp -g -pthread
LDFLAGS += -static
LDLIBS +=  -lglog -lopencv_features2d -lopencv_imgproc -lopencv_imgcodecs -lopencv_flann -lopencv_core -lopencv_hal -lIlmImf -lippicv -lboost_timer -lboost_chrono -lboost_program_options -lboost_thread -lboost_filesystem -lboost_system -lopenblas -ljpeg -ltiff -lpng -ljasper -lwebp -lpthread -lz -lm -lrt -ldl

PROGS = test test2

all:	$(PROGS)

test:	test.o picpoc.o picpoc-io.o

test2:	test2.o picpoc.o picpoc-io.o

clean:
	rm *.o test
