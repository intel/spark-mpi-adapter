
#include <sys/time.h>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <vector>
#include "SparkIntegration.h"
#include "GraphMatRuntime.cpp"
#include "Degree.cpp"

class PR {
public:
  double pagerank;
  int degree;

public:
  PR() {
    pagerank = 0.3;
    degree = 0;
  }
  int operator!=(const PR &p) { return (fabs(p.pagerank - pagerank) > 1e-5); }
  friend std::ostream &operator<<(std::ostream &outstream, const PR &val) {
    outstream << val.pagerank;
    return outstream;
  }
};

template <class E> class PageRank : public GraphProgram<double, double, PR, E> {
public:
  double alpha;

public:
  PageRank(double a = 0.3) {
    alpha = a;
    this->activity = ALL_VERTICES;
  }

  void reduce_function(double &a, const double &b) const { a += b; }
  void process_message(const double &message, const E edge_val,
                       const PR &vertexprop, double &res) const {
    res = message;
  }
  bool send_message(const PR &vertexprop, double &message) const {
    if (vertexprop.degree == 0) {
      message = 0.0;
    } else {
      message = vertexprop.pagerank / (double)vertexprop.degree;
    }
    return true;
  }
  void apply(const double &message_out, PR &vertexprop) {
    vertexprop.pagerank =
        alpha + (1.0 - alpha) * message_out; // non-delta update
  }
};

void main(int argc, char *argv[]) {
  size_t nthreads = omp_get_max_threads();
  printf("nthreads: %d\n", nthreads);
  assert(argc == 5);
  const char *rddBase = argv[1];
  const char *rddBaseOut = argv[2];
  int niter = atoi(argv[3]);
  int shouldFail = atoi(argv[4]);
  struct timeval start, end;
  unsigned long compute_start_time, compute_end_time, build_start_time,
      build_end_time;

  int rank, size;
  MPI_Init(&argc, &argv);
  GraphPad::GB_Init();
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
  GraphPad::edge_t<int> *edges = new GraphPad::edge_t<int>[nnz];
  int _maxv = 0, maxv = 0;
#pragma omp parallel for reduction(max : _maxv)
  for (size_t p = 0; p < partitions.size(); p++) {
    int numRecords = *((const int *)partitions[p].data);
    for (size_t i = 0; i < numRecords; i++) {
      const char *c_inbuf = ((const char *)partitions[p].data) + i * 12 + 4;
      edges[offsets[p] + i].src = *((int *)(c_inbuf + 0));
      edges[offsets[p] + i].dst = *((int *)(c_inbuf + 4));
      edges[offsets[p] + i].val = *((int *)(c_inbuf + 8));
      _maxv = std::max(_maxv, edges[offsets[p] + i].src);
      _maxv = std::max(_maxv, edges[offsets[p] + i].dst);
    }
  }
  delete[] offsets;

  MPI_Barrier(MPI_COMM_WORLD);
  gettimeofday(&start, 0);
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    std::cout << "build start time: " << getTimeInMilliseconds(start)
              << std::endl;
  }

  // Find Graph dimension
  MPI_Allreduce(&_maxv, &maxv, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

  std::cout << "Rank : " << rank << " maxv: " << maxv << std::endl;

  // Create edgelist
  GraphPad::edgelist_t<int> edgelist(edges, maxv, maxv, nnz);

  // Create graph
  Graph<PR, int> G;
  PageRank<int> pr;
  Degree<PR, int> dg;
  G.MTXFromEdgelist(edgelist, nthreads * 8);

  delete[] edgelist.edges;

  MPI_Barrier(MPI_COMM_WORLD);
  gettimeofday(&end, 0);
  MPI_Barrier(MPI_COMM_WORLD);
  double build_time =
      (end.tv_sec - start.tv_sec) * 1e3 + (end.tv_usec - start.tv_usec) * 1e-3;
  if (rank == 0) {
    std::cout << "build_end_time: " << getTimeInMilliseconds(end) << std::endl;
  }

  MPI_Barrier(MPI_COMM_WORLD);
  gettimeofday(&start, 0);
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) {
    std::cout << "compute_start_time: " << getTimeInMilliseconds(start)
              << std::endl;
  }

  G.setAllActive();
  run_graph_program(&dg, G, 1);

  G.setAllActive();
  run_graph_program(&pr, G, niter);

  MPI_Barrier(MPI_COMM_WORLD);
  gettimeofday(&end, 0);
  MPI_Barrier(MPI_COMM_WORLD);
  double pr_time =
      (end.tv_sec - start.tv_sec) * 1e3 + (end.tv_usec - start.tv_usec) * 1e-3;
  if (rank == 0) {
    std::cout << "compute_end_time: " << getTimeInMilliseconds(end)
              << std::endl;
  }

  if (rank == 0) {
    printf("Build Time = %f s \n", build_time / 1000.0);
    printf("Compute Time = %f s \n", pr_time / 1000.0);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  for (int i = 1;
       i <= std::min((unsigned long long int)25,
                     (unsigned long long int)G.getNumberOfVertices());
       i++) {
    if (G.vertexNodeOwner(i)) {
      printf("%d : %d %f\n", i, G.getVertexproperty(i).degree,
             G.getVertexproperty(i).pagerank);
    }
    fflush(stdout);
    MPI_Barrier(MPI_COMM_WORLD);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  gettimeofday(&start, 0);
  MPI_Barrier(MPI_COMM_WORLD);

  GraphPad::edgelist_t<PR> out_edgelist;
  G.getVertexEdgelist(out_edgelist);

  // Serialize
  uint32_t nbytes_out = out_edgelist.nnz * 12;
  void *blob = malloc(nbytes_out);
#pragma omp parallel for
  for (size_t i = 0; i < out_edgelist.nnz; i++) {
    char *c_inbuf = (char *)blob;
    *((int *)(c_inbuf + 12 *i + 0)) = out_edgelist.edges[i].src;
    *((double *)(c_inbuf + 12 *i + 4)) = out_edgelist.edges[i].val.pagerank;
  }

  MPI_Barrier(MPI_COMM_WORLD);
  gettimeofday(&end, 0);
  MPI_Barrier(MPI_COMM_WORLD);

  // Construct NativeRDD for output
  std::vector<SparkIntegration::partition> partitions_out;
  partitions_out.push_back({ rank, nbytes_out, blob });

  SparkIntegration::SaveShm(partitions_out, rddBaseOut);

  free(blob);

  double hdfs_time =
      (end.tv_sec - start.tv_sec) * 1e3 + (end.tv_usec - start.tv_usec) * 1e-3;

  if (rank == 0) {
    printf("Intermediate output write Time = %f s \n", hdfs_time / 1000.0);
  }

  MPI_Finalize();
}
