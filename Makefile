all: bin/Pagerank bin/SaveHDFS

MPICXX=mpicxx -cxx=icpc
HADOOP_PATH=/export/swtools/hadoop
GMPATH=../GraphMat/src
GPPATH=../GraphMat/GraphMatDistributedPrimitives
GMFLAGS=-I$(GMPATH) -I$(GPPATH)
CFLAGS=$(GMFLAGS) -std=c++11 -qopenmp -O2 -I$(HADOOP_PATH)/include -ipo
LDFLAGS=-L$(HADOOP_PATH)/lib/native -lhdfs

bin/Pagerank: bin/ src/Pagerank.cc src/SparkIntegration.h
	$(MPICXX) $(CFLAGS) src/Pagerank.cc -o bin/Pagerank $(LDFLAGS)

bin/SaveHDFS: bin/ src/SaveHDFS.cc src/SparkIntegration.h
	$(MPICXX) $(CFLAGS) src/SaveHDFS.cc -o bin/SaveHDFS $(LDFLAGS)

bin/:
	mkdir -p bin

clean:
	rm -rf ./bin
