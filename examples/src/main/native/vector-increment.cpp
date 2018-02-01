
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <vector>
#include "spark-integration.h"

int main(int argc, char *argv[]) {
  assert(argc == 3);
  const char *rddBase = argv[1];
  const char *rddBaseOut = argv[2];

  int rank, size;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  auto partitions = SparkIntegration::binaryFiles(rddBase);

  // Deserialize each partition
  int nnz = 0;
  size_t *offsets = new size_t[partitions.size() + 1];
  offsets[0] = 0;
  for (size_t p = 0; p < partitions.size(); p++) {
    int numRecords = *((const int *)partitions[p].data);
    offsets[p + 1] = offsets[p] + (size_t)numRecords;
    nnz += numRecords;
  }
  std::cout << "Rank : " << rank << " nnz: " << nnz << std::endl;
  int * edges = new int[nnz];
  for (size_t p = 0; p < partitions.size(); p++) {
    int numRecords = *((const int *)partitions[p].data);
    for (size_t i = 0; i < numRecords; i++) {
      const char *c_inbuf = ((const char *)partitions[p].data) + i * 4 + 4;
      edges[offsets[p] + i] = *((int *)(c_inbuf + 0));
    }
  }
  delete[] offsets;

  for(size_t i = 0 ; i < nnz ; i++)
  {
    edges[i]++;
    printf("EDGE %d %d\n", i, edges[i]);
  }

  // Serialize out
  uint32_t nbytes_out = nnz * 4;
  void *blob = malloc(nbytes_out);
  for (size_t i = 0; i < nnz; i++) {
    char *c_inbuf = (char *)blob;
    *((int *)(c_inbuf + 4 *i + 0)) = edges[i];
  }

  // Construct NativeRDD for output
  std::vector<SparkIntegration::partition> partitions_out;
  partitions_out.push_back({ (uint32_t)rank, nbytes_out, blob });

  SparkIntegration::SaveShm(partitions_out, rddBaseOut);

  free(blob);
  MPI_Finalize();
  return 0;
}
