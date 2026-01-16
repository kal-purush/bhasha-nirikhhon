import numpy
import random
import matplotlib.pyplot as pplot

random.seed(0)

run_tests = [10, 100, 1000, 10000, 100000]
data_in = []
means = []
stds = []
runs = []
scale = .1

def main():
  with open('abalone.data') as file:
    num_rows = sum(1 for r in file)
    data_in = [0]*num_rows

  for r in run_tests:
    data_in = [0]*len(data_in)
    for i in range(r):
      row_len = len(data_in)
      n_n = float(int(row_len*scale))
      n_r = row_len
      for j, k in enumerate(data_in):
        if n_n/n_r > random.random():
          data_in[j] += 1
          n_n -= 1
        n_r -= 1

    means.append(numpy.mean(data_in)/r)
    stds.append(numpy.std(data_in)/r)
    runs.append(r)

  pplot.subplot(2, 1, 1)
  pplot.plot(runs, means)
  pplot.title("Mean VS Runs")
  pplot.ylabel("mean")
  
  pplot.subplot(2, 1, 2)
  pplot.plot(runs, stds)
  pplot.title("Standard deviation VS Runs")
  pplot.xlabel("runs")
  pplot.ylabel("standard deviation")
  
  pplot.show()

main()