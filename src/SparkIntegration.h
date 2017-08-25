#ifndef __SPARK_INTEGRATION_H
#define __SPARK_INTEGRATION_H

#include <mpi.h>
#include <vector>
#include <cstring>
#include <functional>
#include <cassert>
#include <sstream>
#include <fstream>
#include <sys/mman.h>
#include <fcntl.h>
#include <cstdio>
#include <omp.h>
#include <unistd.h>
#include <cerrno>
#include "hdfs.h"

unsigned long getTimeInMilliseconds(struct timeval t) {
  return ((unsigned long)t.tv_sec) * ((unsigned long)1000) +
         ((unsigned long)t.tv_usec) / ((unsigned long)1000);
}

namespace SparkIntegration {

struct partition {
  uint32_t id;
  uint32_t num_bytes;
  const void *data;
};

std::vector<partition>
binaryFiles(const char *rddBase) // change this to return edge pointer
{
  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  std::vector<partition> partitions;

  std::vector<uint32_t> ranks;
  std::vector<uint32_t> pnums;
  std::vector<uint32_t> sizes;

  if (rank == 0) {
    std::stringstream partitions_ss;
    partitions_ss << rddBase << "partitions";
    FILE *partition_file = fopen(partitions_ss.str().c_str(), "r");
    assert(partition_file);

    uint32_t rank, partition_num, partition_size;
    while (fscanf(partition_file, "%d,%d,%d\n", &rank, &partition_num,
                  &partition_size) > 0) {
      ranks.push_back(rank);
      pnums.push_back(partition_num);
      sizes.push_back(partition_size);
      std::cout << rank << "\t" << partition_num << "\t" << partition_size
                << std::endl;
    }
    fclose(partition_file);
  }
  uint32_t num_partitions = ranks.size();
  MPI_Bcast(&num_partitions, 1, MPI_INT, 0, MPI_COMM_WORLD);
  if (rank != 0) {
    ranks = std::vector<uint32_t>(num_partitions);
    pnums = std::vector<uint32_t>(num_partitions);
    sizes = std::vector<uint32_t>(num_partitions);
  }
  MPI_Bcast(ranks.data(), num_partitions, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(pnums.data(), num_partitions, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Bcast(sizes.data(), num_partitions, MPI_INT, 0, MPI_COMM_WORLD);

  assert(ranks.size() == pnums.size());
  assert(pnums.size() == sizes.size());
  for (size_t i = 0; i < ranks.size(); i++) {
    if (ranks[i] == rank) {
      std::stringstream ss;
      ss << rddBase << i;
      std::cout << "Rank: " << rank << " Opening: " << ss.str() << std::endl;
      int fd = open(ss.str().c_str(), O_RDONLY);
      std::cout << "errno: " << strerror(errno) << std::endl;
      assert(fd != -1);
      unsigned long int nbytes = sizes[i];
      void *edge_addr =
          mmap(NULL, nbytes, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
      partitions.push_back({ i, sizes[i], edge_addr });
    }
  }
  return partitions;
}

/*
 * template <typename T>
void NativeRDD<T>::deleteFiles() {
  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  for(auto it = partitions.begin() ; it != partitions.end() ; it++)
  {
    std::stringstream ss;
    ss << rddBase << it->id;
    remove(ss.str().c_str());
    std::cout << "deleting: " << ss.str() << std::endl;
  }
  if(rank == 0)
  {
    {
      std::stringstream ss;
      ss << rddBase << "partitions";
      //remove(ss.str().c_str());
    }
    {
      std::stringstream ss;
      ss << rddBase << "hostfile";
      //remove(ss.str().c_str());
    }
  }
}
*/

void SaveShm(std::vector<partition> partitions, const char *rddBaseOut) {
  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  std::cout << "size: " << size << std::endl;

  // Get num partitions from each process
  int local_partitions = partitions.size();
  int *num_partitions = new int[size];
  MPI_Allgather(&local_partitions, 1, MPI_INT, num_partitions, 1, MPI_INT,
                MPI_COMM_WORLD);

  int *partition_offsets = new int[size + 1];
  partition_offsets[0] = 0;
  for (int i = 0; i < size; i++) {
    partition_offsets[i + 1] = partition_offsets[i] + num_partitions[i];
  }

  // Gather partition information
  int *my_partition_ids = new int[partitions.size()];
  int *my_partition_sizes = new int[partitions.size()];
  int *my_partition_ranks = new int[partitions.size()];
  char my_hostname[255];
  gethostname(my_hostname, 255);

  for (int i = 0; i < partitions.size(); i++) {
    my_partition_ids[i] = partitions[i].id;
    my_partition_sizes[i] = partitions[i].num_bytes;
    my_partition_ranks[i] = rank;
  }

  int *partition_ids = new int[partition_offsets[size]];
  int *partition_sizes = new int[partition_offsets[size]];
  int *partition_ranks = new int[partition_offsets[size]];
  char *hostnames = new char[255 * size];
  MPI_Gatherv(my_partition_ids, partitions.size(), MPI_INT, partition_ids,
              num_partitions, partition_offsets, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Gatherv(my_partition_sizes, partitions.size(), MPI_INT, partition_sizes,
              num_partitions, partition_offsets, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Gatherv(my_partition_ranks, partitions.size(), MPI_INT, partition_ranks,
              num_partitions, partition_offsets, MPI_INT, 0, MPI_COMM_WORLD);
  MPI_Gather(my_hostname, 255, MPI_CHAR, hostnames, 255, MPI_CHAR, 0,
             MPI_COMM_WORLD);

  if (rank == 0) {
    // Write hostfile
    std::stringstream hostfilename;
    hostfilename << rddBaseOut << "hostfile";
    std::ofstream hostfile_stream;
    hostfile_stream.open(hostfilename.str().c_str());
    for (int i = 0; i < size; i++) {
      const char *str = hostnames + 255 * i;
      hostfile_stream << str << std::endl;
    }
    hostfile_stream.close();

    // Write partition file
    std::stringstream partitionfilename;
    partitionfilename << rddBaseOut << "partitions";
    std::ofstream partitionfile_stream;
    partitionfile_stream.open(partitionfilename.str().c_str());
    for (int i = 0; i < partition_offsets[size]; i++) {
      partitionfile_stream << partition_ranks[i] << "," << partition_ids[i]
                           << "," << partition_sizes[i] << std::endl;
    }
    partitionfile_stream.close();
  }

  delete[] my_partition_ids;
  delete[] my_partition_sizes;
  delete[] my_partition_ranks;
  delete[] partition_ids;
  delete[] partition_sizes;
  delete[] partition_ranks;
  delete[] partition_offsets;
  delete[] num_partitions;

  // Write my partitions
  for (int i = 0; i < partitions.size(); i++) {
    // open file
    std::stringstream ss;
    ss << rddBaseOut << partitions[i].id;
    std::cout << "Rank: " << rank << " writing: " << ss.str() << std::endl;
    int fd = open(ss.str().c_str(), O_RDWR | O_CREAT, 0666);
    assert(fd != -1);
    if(partitions[i].num_bytes > 0)
    {
      int fallocate_return = fallocate(fd, 0, 0, partitions[i].num_bytes);
      assert(fallocate_return != -1);
      void *edge_addr =
          mmap(NULL, partitions[i].num_bytes, PROT_READ | PROT_WRITE,
               MAP_SHARED | MAP_POPULATE, fd, 0);
      memcpy(edge_addr, partitions[i].data, partitions[i].num_bytes);
      munmap(edge_addr, partitions[i].num_bytes);
    }
    close(fd);
  }
}

void SaveHDFS(std::vector<partition> partitions, const char *rddBaseOut) {
  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  struct timeval tv_start, tv_end;
  double time;
  gettimeofday(&tv_start, 0);
  hdfsFS handle = hdfsConnect("default", 0);
  gettimeofday(&tv_end, 0);
  time = (tv_end.tv_sec - tv_start.tv_sec) * 1e3 +
         (tv_end.tv_usec - tv_start.tv_usec) * 1e-3;
  std::cout << "handle time: " << time << std::endl;

  gettimeofday(&tv_start, 0);
#pragma omp parallel for
  for (size_t p = 0; p < partitions.size(); p++) {
    if (partitions[p].num_bytes > 0) {
      std::stringstream outname;
      outname << rddBaseOut << "_" << partitions[p].id;
      hdfsFile f = hdfsOpenFile(handle, outname.str().c_str(),
                                O_WRONLY | O_CREAT, 0, 0, 0);
      assert(f);
      hdfsWrite(handle, f, partitions[p].data, partitions[p].num_bytes);
      assert(!hdfsFlush(handle, f));
    }
  }
  gettimeofday(&tv_end, 0);
  time = (tv_end.tv_sec - tv_start.tv_sec) * 1e3 +
         (tv_end.tv_usec - tv_start.tv_usec) * 1e-3;
  std::cout << "write time: " << time << std::endl;

  gettimeofday(&tv_start, 0);
  hdfsDisconnect(handle);
  gettimeofday(&tv_end, 0);
  time = (tv_end.tv_sec - tv_start.tv_sec) * 1e3 +
         (tv_end.tv_usec - tv_start.tv_usec) * 1e-3;
  std::cout << "disconnect time: " << time << std::endl;
}
}

#endif // __SPARK_INTEGRATION_H
