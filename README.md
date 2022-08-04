DISCONTINUATION OF PROJECT.

This project will no longer be maintained by Intel.

Intel has ceased development and contributions including, but not limited to, maintenance, bug fixes, new releases, or updates, to this project. 

Intel no longer accepts patches to this project.

If you have an ongoing need to use this project, are interested in independently developing it, or would like to maintain patches for the open source software community, please create your own fork of this project. 
# MPI Adapter for Apache Spark

See our VLDB 2017 paper:

Michael Anderson, Shaden Smith, Narayanan Sundaram, Mihai Capotă, Zheguang Zhao, Subramanya Dulloor, Nadathur Satish, & Theodore L. Willke. 2017. Bridging the gap between HPC and big data frameworks. Proc. VLDB Endow. 10, 8 (April 2017), 901-912.

https://doi.org/10.14778/3090163.3090168

http://www.vldb.org/pvldb/vol10/p901-anderson.pdf

## Prerequisites

1. `HADOOP_HDFS_HOME` environment variable set to the Hadoop install directory,
   such that `HADOOP_HDFS_HOME/include` contains `hdfs.h` and
   `HADOOP_HDFS_HOME/lib/native` contains `libhdfs.so`.

2. Spark running in standalone mode. Other modes are not supported.

## Installation

1. Install Scala library

```
sbt publishLocal
```

2. Install library & helper script

```
mkdir build
cd build
cmake ..
make
cp ../src/main/native/*.h .
cp ../scripts/*.sh .
export MPI_ADAPTER_FOR_SPARK_HOME=$(pwd)
cd ..
```

## Examples

1. Build example Scala library

```
cd examples
sbt package
```

2. Build example native code

```
mkdir build
cd build
cmake ..
make
export MPI_ADAPTER_FOR_SPARK_EXAMPLES=$(pwd)
cd ..
```

3. Run

```
spark-submit --jars \
../target/scala-2.10/spark-mpi-adapter_2.10-0.1-SNAPSHOT.jar \
target/scala-2.10/spark-mpi-adapter-examples_2.10-0.1-SNAPSHOT.jar \
com.intel.MPIAdapterForSpark.VectorIncrement
```
