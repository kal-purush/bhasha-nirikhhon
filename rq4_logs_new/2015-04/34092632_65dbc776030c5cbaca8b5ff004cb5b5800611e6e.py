#!/usr/bin/env python

import os
import argparse
import subprocess
import datetime
import re

def parse_args():
    parser = argparse.ArgumentParser(description="Run and time the project Euler solutions for various languages")
    parser.add_argument('-p', '--problem', type=int, help="Run a specific problem")
    parser.add_argument('-l', '--language', type=str, help="Run a specific language")
    parser.add_argument('-v', '--verbose', action="store_true", help="Debug output")
    return parser.parse_args()

def make_run_cmd(problem):
    cmd = 'bash run.sh {} 2>&1'.format(problem)
    return cmd

def run_problem(problem, languages):
    cmd = make_run_cmd(problem)
    print "{}".format(problem)
    print "\t{:15}\t{:10}{:10}".format("Language", "Minutes", "Seconds")
    results = []
    for language in languages:
        out = subprocess.check_output([cmd], shell=True, cwd=os.path.join(os.getcwd(), language))
        minutes,seconds = get_time(out)
        results.append((language, minutes, seconds))

    results.sort(key=lambda x: (x[1], x[2]))

    for language, minutes,seconds in results:
        print "\t{:15}\t{:10}{:10}".format(
            language, 
            minutes,
            seconds
        )

def get_time(buf):
    rgx = 'real\s+([0-9]+)m([0-9]+\.[0-9]+)s'
    m = re.search(rgx, buf)
    if not m:
        print "Something went wrong, couldn't match buffer {}".format(buf)
        return
    minutes = m.group(1)
    seconds = m.group(2)
    
    return (minutes, seconds)

def get_problems(languages):
    '''
    Searches through all language directories to get the set of problems that
    should be run
    '''
    problems = set()
    for language in languages:
        listdir = os.path.join(os.getcwd(), language)
        dirs = [x for x in os.listdir(listdir) if os.path.isdir(os.path.join(listdir, x))]
        dirs = [x for x in dirs if not x.startswith('.')]
        #Verify its a problem (integer)
        for d in dirs:
            try:
                int(d)
                problems.update(d)
            except (ValueError, TypeError):
                print "Bad problem directory {}".format(d)
    problems = list(problems)
    problems.sort()
    return problems
         

def get_languages():
    '''
    Interprets each directory at the top level of the repo as a language
    '''
    #return [os.path.basename(x[0]) for x in os.walk('.') if not os.path.basename(x[0]).startswith('.')]
    dirs = [os.path.basename(x) for x in os.listdir(os.getcwd()) if os.path.isdir(x)]
    return [x for x in dirs if not x.startswith('.')]

def main():
    args = parse_args()
    if args.language:
        languages = [args.language]
    else:
        languages = get_languages()

    if args.verbose:
        print "Running languages {}".format(",".join(languages))

    if args.problem:
        run_problem(args.problem, languages)
        return
    else:
        problems = get_problems(languages)    


    if args.verbose:
        print "Running problems {}".format(",".join(problems))

    for problem in problems:
        run_problem(problem, languages)

if __name__ == "__main__":
    main()