#!/usr/bin/python3

import os
import subprocess
import argparse

def generate_test_case_output(solution, timeout, inputfile, outputfile):
    ext = solution.split(".")[-1]
    print("Running {} on {}".format(solution, inputfilename))
    if ext == "py":
        proc = subprocess.Popen(["python", solution], stdin=inputfile, stdout=outputfile)
        proc.communicate(timeout=timeout)
        proc.wait()
        if proc.returncode != 0:
            print("----Error: Return code='{}'".format(proc.returncode))
    elif ext == "cpp":
        subprocess.Popen(["g++", "-std=c++11", solution]).communicate()
        proc = subprocess.Popen(["./a.out"], stdin=inputfile, stdout=outputfile)
        proc.communicate(timeout=timeout)
        proc.wait()
        subprocess.Popen(["rm", "a.out"]).communicate()
        if proc.returncode != 0:
            print("----Error: Return code='{}'".format(proc.returncode))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test Generator.')
    parser.add_argument('scriptfile', help='Name of script file to test')
    parser.add_argument('-timeout', "-t", help='Time limit for solutions', default=2)
    args = parser.parse_args()
    scriptfile = args.scriptfile
    timeout = float(args.timeout)
    test_dir = "tests"
    input_dir = test_dir + "/input"
    output_dir = test_dir + "/output"
    input_template = input_dir + "/input{:02}.txt"
    output_template = output_dir + "/output{:02}.txt"

    # iterate through the input files for the test cases
    for inputfilename in os.listdir(input_dir):
        inputfilename = input_dir + "/" + inputfilename
        case = inputfilename[-6:]
        outputfilename = output_dir + "/output" + case
        with open(inputfilename) as inputfile, open(outputfilename, "w") as outputfile:
            generate_test_case_output(scriptfile, timeout, inputfile, outputfile)