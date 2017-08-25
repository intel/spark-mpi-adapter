
#include <sys/time.h>
#include <iostream>
#include "SparkIntegration.h"

void main(int argc, char *argv[]) {
  assert(argc == 3);
  const char *rddBase = argv[1];
  const char *rddBaseOut = argv[2];
  struct timeval start, end;

  int rank, size;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  MPI_Barrier(MPI_COMM_WORLD);
  gettimeofday(&start, 0);
  MPI_Barrier(MPI_COMM_WORLD);

  auto partitions = SparkIntegration::binaryFiles(rddBase);

  std::stringstream outstream;
  outstream << rddBaseOut << "/file";
  std::cout << "Writing to " << outstream.str() << std::endl;
  SparkIntegration::SaveHDFS(partitions, outstream.str().c_str());

  MPI_Barrier(MPI_COMM_WORLD);
  gettimeofday(&end, 0);
  MPI_Barrier(MPI_COMM_WORLD);
  double hdfs_time =
      (end.tv_sec - start.tv_sec) * 1e3 + (end.tv_usec - start.tv_usec) * 1e-3;

  if (rank == 0) {
    printf("HDFS write Time = %f s \n", hdfs_time / 1000.0);
  }

  MPI_Finalize();
}
