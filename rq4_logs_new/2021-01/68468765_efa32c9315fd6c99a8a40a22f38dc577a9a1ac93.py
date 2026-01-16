import sys
import os
import re
from statistics import mean, stdev

def read_from_file(filename, keys):
    inf = open(filename, 'r')
    values = dict()
    for key in keys:
        values[key] = []
    for line in inf.readlines():
        ll = line.lower()
        good = None
        for key in keys:
            if key.lower() in ll:
                good = key
        if good == None:
            continue
        value = ll.split(":")[1].strip().split(" ")[1][1:]
        values[key].append(float(value))
    return values

def filename(bench, size, write, proc):
   return "../output/log/" + bench + "-" + str(proc) + "-" + str(size) + "-" + str(write)

keys = ["#txs"]
procs = range(1, 80)#[1, 2, 4, 8, 15, 16, 23, 24, 31, 32, 39, 47, 55, 63, 71, 79]
sizes = [25, 100, 1000, 10000]
writes = [0, 20, 40, 60, 80, 100]

benchmarks = ["co-list-v2",
              "lazy-list-v2",
              "harris"
              ]

directory = "../output/data_files/"
if not os.path.isdir(directory):
    os.mkdir(directory)

for key in keys:
    for size in sizes:
        for write in writes:
            for i in range(len(benchmarks)):
                bench = benchmarks[i]
                if not os.path.exists(filename(bench, size, write, 1)):
                    continue
                out = open(directory + "comparison_" + str(key)[1:] + "_" + str(size) + "_" + str(write) + "_" + str(i) + ".dat", 'w')
                for proc in procs:
                    if not os.path.exists(filename(bench, size, write, proc)):
                        continue
                    results = read_from_file(filename(bench, size, write, proc), keys)[key]
                    if len(results) == 10:
                        results = results[5:]
                    results = results[1:]
                    out.write(str(proc) + " " + str(mean(results) / 1000 / 1000) + " " + str(stdev(results) / 1000 / 1000) + "\n")

directory = "../output"
if not os.path.isdir(directory):
    os.mkdir(directory)

path_id = 0
for key in keys:
    for size in sizes:
        out = open(directory + "/comparison_" + str(key) + "_" + str(size) + ".plot", 'w')
        for write in writes:
            out.write("\\begin{tikzpicture}\n")
            out.write("\\begin{axis}[\nlegend style={at={(0.5, -0.1)},anchor=north},\n xlabel={Processors},\n ylabel={Throughput mops/s},\n" +
                      " cycle list name=color,\n title=Update rate: " + str(write) + "\\%\n]\n")
            for i in range(len(benchmarks)):
                bench = benchmarks[i]
                data_file = "data/dasquad/cpp/data_files/trees_comparison_" + str(key) + "_" + str(size) + "_" + str(write) + "_" + str(i) + ".dat"
                if not os.path.exists(filename(bench, size, write, 1)):
                    continue
#  Version with error bars
#                if bench in error_bars:
#                    out.write("\\addplot+[error bars/.cd, y dir=both, y explicit] table [x index = 0, y index = 1, y error index = 2] {" + data_file + "};\n")
#                else:
# Version with filled curve
#                if True: #bench in error_bars:
#                    out.write("\\addplot [name path=pluserror,draw=none,no markers,forget plot] table [x index = 0, y expr=\\thisrowno{1}+\\thisrowno{2}] {" + data_file + "};\n")
#                    out.write("\\addplot [name path=minuserror,draw=none,no markers,forget plot] table [x index = 0, y expr=\\thisrowno{1}-\\thisrowno{2}] {" + data_file + "};\n")
#                    out.write("\\addplot+[opacity=0.3,forget plot] fill between [of = pluserror and minuserror];\n")
                out.write("\\addplot table [x index = 0, y index = 1] {" + data_file + "};\n")
                out.write("\\addlegendentry{" + bench + "};\n")

            out.write("\\end{axis}\n")
            out.write("\\end{tikzpicture}\n")


out.close()
