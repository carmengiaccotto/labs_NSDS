#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

// Group members:
// - Alessia Franchetti-Rosada
// - Carmen Giaccotto
// - Alessandro Paolo Gianni Callegari

const int N = 256;

// Allocates and initializes matrix
int* generate_matrix() {
  int* A = (int*) malloc(N * N * sizeof(int));
  for (int i = 0; i < N * N; i++) {
    A[i] = rand() % 100;
  }
  return A;
}

// Returns the value at the given row and column
int val(int *A, int r, int c) {
  return A[r * N + c];
}

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);

  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  if (N % size != 0) {
    if (rank == 0) printf("N must be a multiple of the number of processes.\n");
    MPI_Finalize();
    return 0;
  }

  int rows_per_proc = N / size;
  int local_size = rows_per_proc * N;

  // ------------------------------------------------------------
  // Generate matrix on rank 0
  // ------------------------------------------------------------

  int* fullA = NULL;
  if (rank == 0) {
      fullA = generate_matrix();
  }

  // ------------------------------------------------------------
  // Distribute to all processes
  // ------------------------------------------------------------

  int* local = (int*) malloc(local_size * sizeof(int));

  MPI_Scatter(fullA, local_size, MPI_INT, local, local_size, MPI_INT, 0, MPI_COMM_WORLD);

  // ------------------------------------------------------------
  // Free global matrix
  // ------------------------------------------------------------

  if (rank == 0) {
      free(fullA);
  }

  // ------------------------------------------------------------
  // Exchange further information if needed
  // ------------------------------------------------------------

  int* row_above = (int*) malloc(N * sizeof(int));
  int* row_below = (int*) malloc(N * sizeof(int));

  int neighbor_up = rank - 1;
  int neighbor_down = rank + 1;

  if (rank != 0) {
      MPI_Send(&local[0], N, MPI_INT, neighbor_up, 0, MPI_COMM_WORLD);
  }

  if (rank != size - 1) {
      MPI_Recv(row_below, N, MPI_INT, neighbor_down, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

  if (rank != size - 1) {
      int last_row_idx = (rows_per_proc - 1) * N;
      MPI_Send(&local[last_row_idx], N, MPI_INT, neighbor_down, 1, MPI_COMM_WORLD);
  }

  if (rank != 0) {
      MPI_Recv(row_above, N, MPI_INT, neighbor_up, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

  // ------------------------------------------------------------
  // Compute local minima (excluding GLOBAL borders)
  // ------------------------------------------------------------

  int* local_counts = (int*) calloc(rows_per_proc, sizeof(int));

  for (int i = 0; i < rows_per_proc; i++) {
      int global_row_idx = rank * rows_per_proc + i;

      if (global_row_idx == 0 || global_row_idx == N - 1) {
          local_counts[i] = 0;
      } else {
          for (int j = 1; j < N - 1; j++) {
              int current = val(local, i, j);
              int up, down, left, right;

              left = val(local, i, j - 1);
              right = val(local, i, j + 1);

              if (i == 0) {
                  up = row_above[j];
              } else {
                  up = val(local, i - 1, j);
              }

              if (i == rows_per_proc - 1) {
                  down = row_below[j];
              } else {
                  down = val(local, i + 1, j);
              }

              if (current < left && current < right && current < up && current < down) {
                  local_counts[i]++;
              }
          }
      }
  }

  // ------------------------------------------------------------
  // Send results to rank 0 and print results on rank 0
  // ------------------------------------------------------------

  int* global_counts = NULL;
  if (rank == 0) {
      global_counts = (int*) malloc(N * sizeof(int));
  }

  MPI_Gather(local_counts, rows_per_proc, MPI_INT, global_counts, rows_per_proc, MPI_INT, 0, MPI_COMM_WORLD);

  if (rank == 0) {
      for (int i = 0; i < N; i++) {
          printf("Row: %d, local minima: %d\n", i, global_counts[i]);
      }
      free(global_counts);
  }

  // ------------------------------------------------------------
  // Free allocated memory
  // ------------------------------------------------------------

  free(local);
  free(row_above);
  free(row_below);
  free(local_counts);

  MPI_Finalize();
  return 0;
}