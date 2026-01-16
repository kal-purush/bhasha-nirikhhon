#!/usr/bin/env python3
"""
Check coverage by patterns.

File: patterns.py

Copyright 2016 Ankur Sinha
Author: Ankur Sinha <sanjay DOT ankur AT gmail DOT com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import random
import math
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


random.seed(42)

pattern_percent = 0.1
output_filename = ("pattern-coverage.png")
matplotlib.rcParams.update({'font.size': 20})
plt.figure(num=None, figsize=(16, 9), dpi=80)
plt.ylabel("Coverage (neurons in patterns/total available neurons)")
plt.xlabel("number of patterns")
plt.title("Coverage by patterns (800 neurons) of a network (8000 neurons)")
ax = plt.gca()
ax.ticklabel_format(useOffset=False)

neurons = [i for i in range(0, 8000)]

for run in range(0, 25):
    numpats = [i for i in range(5, 110, 2)]
    coverage = []
    for numpat in numpats:
        allpats = []
        for i in range(0, numpat):
            pattern = random.sample(neurons,
                                    int(math.ceil(float(len(neurons)) *
                                                  pattern_percent)))
            allpats += pattern
            allpats = list(set(allpats))
        coverage.append(float(len(allpats))/float(len(neurons)))

    plt.plot(numpats, coverage, ".", markersize=5)

plt.legend(loc="upper right")
plt.savefig(output_filename)
plt.close()