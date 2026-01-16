#!/usr/bin/env python3

import argparse
import numpy as np
import matplotlib.pyplot as plt
import json
import os
import sys

"""
    @return list
    [
     { browserName,
       browserVersion,
       results : [
                  { benchmarkName,
                    reaultValue
                  },
                  ...
                 ]
     },
     ...
    ]
"""

def cli(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('files', metavar='N', type=str, nargs='+',
                        help='an file for the visualization')
    args = parser.parse_args()

    sources = []
    for file in args.files:
        with open(file) as fp:
                source = json.load(fp)
                sources.append(source)

    width = 0.2
    fig, ax = plt.subplots()
    caseNameList = []
    caseValueLists = []

    for browserName in sources[0]["browsers"]:
        for suite in sources[0]["browsers"][browserName]["suites"]:
            caseName = suite["result_name"]

            for result in suite["results"]:
                for case in result:
                    caseNameList.append(case[caseName])
                break
            break
        break

    for source in sources:
        for browserName in source["browsers"]:
            caseValueList = []
            for suite in source["browsers"][browserName]["suites"]:
                caseValue = suite["result_value"]

                for result in suite["results"]:
                    for case in result:
                        caseValueList.append(case[caseValue])
            caseValueLists.append(caseValueList)

    numberOfCase = len(caseNameList)
    ind = np.arange(numberOfCase)  # the x locations for the groups

    ratios = []
    for caseValueList in caseValueLists[1:]:
        for i in range(0, numberOfCase):
            ratios.append(caseValueList[i] / caseValueLists[0][i])

    refRects = ax.bar(ind, np.ones(numberOfCase), width, color='g')
    rects = ax.bar(ind+width, ratios, width, color='r')

    # add some text for labels, title and axes ticks
    ax.set_title('Scores by group and gender')
    ax.set_ylabel('Scores')
    ax.set_xticks(ind+width)
    ax.set_xticklabels(caseNameList, rotation=80)

    plt.show()

if __name__ == "__main__":
    exit(cli(sys.argv[1:]))