import matplotlib.pyplot as plt
import pandas as pd
import os
import sys

PATH = "../"
EXTENSION = ".csv"
MAKESPAN = "_makespan"
RUNTIME = "_runtime"

# Find ".csv" files and add benchmarks names to the list
benchmarks = []
for filename in os.listdir(PATH):
    if filename.endswith(EXTENSION):
        name = filename[:-len(EXTENSION)]
        if name.endswith(MAKESPAN):
            name = name[:-len(MAKESPAN)]
            if name not in benchmarks:
                benchmarks.append(name)
        if name.endswith(RUNTIME):
            name = name[:-len(RUNTIME)]
            if name not in benchmarks:
                benchmarks.append(name)

# Plot makespans and runtimes
i = 0
name = "None"
figure, axes = plt.subplots(2, len(benchmarks), sharex='col', sharey='row')
for benshmark in benchmarks:
    path_makespan = PATH + benshmark + MAKESPAN + EXTENSION
    path_runtime = PATH + benshmark + RUNTIME + EXTENSION

    if os.path.isfile(path_makespan):
        name = benshmark + MAKESPAN

        data = pd.read_csv(path_makespan, delimiter=';')

        # Uses the first column for the x axes
        ax = data.plot(x=data.columns[0], ax=axes[0, i], title=name, marker='o', xticks=data.iloc[:, 0])
        ax.set_xlabel('Problem size', fontsize='x-large')
        ax.set_ylabel('Execution steps', fontsize='x-large')

        # setting font sizes
        ax.legend(fontsize='x-large')
        plt.yticks(fontsize='x-large')
        plt.xticks(fontsize='x-large')

    if os.path.isfile(path_runtime):
        name = benshmark + RUNTIME

        data = pd.read_csv(path_runtime, delimiter=';')

        # Uses the first column for the x axes
        ax = data.plot(x=data.columns[0], ax=axes[1, i], title=name, marker='o', xticks=data.iloc[:, 0])
        ax.set_xlabel('Problem size', fontsize='x-large')
        ax.set_ylabel('Execution time', fontsize='x-large')

        # setting font sizes
        ax.legend(fontsize='x-large')
        plt.yticks(fontsize='x-large')
        plt.xticks(fontsize='x-large')

    i += 1


# filename for the output
outname = "results.pdf"

plt.savefig(outname, format='pdf', dpi=1200)

plt.show()