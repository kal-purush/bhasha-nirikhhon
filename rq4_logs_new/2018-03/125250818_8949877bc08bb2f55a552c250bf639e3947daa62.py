#!/usr/bin/env python3
import sys
import pickle
from mpi4py import MPI


if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    results = {}

    for send_mode_name, send_function in {
            "send": lambda x: comm.send(x, dest=1),
            "bsend": lambda x: comm.bsend(x, dest=1)
    }.items():

        mode_results = {}
        iteration_count = 10000

        for string_len in range(1, 10):
            payload = "a" * string_len
            payload_size = len(pickle.dumps(payload))
            
            comm.Barrier()
            begin_time = MPI.Wtime()
            for _ in range(iteration_count):
                if rank == 0:
                    send_function(payload)
                elif rank == 1:
                    comm.recv(source=0)
            comm.Barrier()
            end_time = MPI.Wtime()
            mode_results[payload_size] = end_time-begin_time
            
        results[send_mode_name] = mode_results

    print(results)