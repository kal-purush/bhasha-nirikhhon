# 12/05/2023
# Start: 7:42PM -06:00
# End  :  9:09PM -06:00
# Horribly inefficient with memory
# TODO: Instead of making huge lists, try to find a clever way to map the values that way
# /u/krowvin
# https://adventofcode.com/2023/day/5

import sys

# Enable extra output
DEBUG = False
INPUT_FILE = "input.txt"

# Run test_input.txt and see if you get the expected lowest location of 35:
# Seed 79, soil 81, fertilizer 81, water 81, light 74, temperature 78, humidity 78, location 82.
# Seed 14, soil 14, fertilizer 53, water 49, light 42, temperature 42, humidity 43, location 43.
# Seed 55, soil 57, fertilizer 57, water 53, light 46, temperature 82, humidity 82, location 86.
# Seed 13, soil 13, fertilizer 52, water 41, light 34, temperature 34, humidity 35, location 35.


almanac = {}    
seeds = []

# Import the file data
def readInputFile(filename: str):
    print(f"Reading {filename}...")
    with open(filename) as input_file:
        # Remove any empty lines
        return [l.strip() for l in input_file.readlines()]

def parseInput(lines: str):
    global seeds
    a_map = "overflow"
    for line in lines:
        if not line: continue
        if line.find(" map:") >= 0:
            a_map = line.split(" map:")[0]
        elif line.startswith("seeds: "):
            seeds = list(map(int, line.split(": ")[1].split(" ")))
        else:
            # dest range start, source range start, range length
            drs, srs, rl = list(map(int, line.split(" ")))
            if a_map not in almanac:
                almanac[a_map] = {
                    "dst": [x for x in range(drs, drs + rl)],
                    "src": [x for x in range(srs, srs + rl)]
                }
            else:
                almanac[a_map]["dst"].extend([x for x in range(drs, drs + rl)])
                almanac[a_map]["src"].extend([x for x in range(srs, srs + rl)])

def getDestination(a_map, src):
    try:
        return almanac[a_map]["dst"][almanac[a_map]["src"].index(src)]
    except ValueError as err:
        return src


def run():
    # Populate the almanac with our data
    parseInput(readInputFile("input.txt"))
    print('parse done')
    output = {"seeds": [], "locations": []}
    for seed in seeds:
        output_str = ""
        dst = seed
        output[seed] = None
        output_str += f"Seed {seed}, "
        for entry in almanac:
            # Lookup soil from seed
            dst = getDestination(entry, dst)
            output_str += f'{entry.split("-")[-1]} {dst}, '
        print("".join(output_str[:-2]))
        # Set the seed location
        output["seeds"].append(seed)
        output["locations"].append(dst)
    lowest_loc = min(output["locations"])
    print(f'The lowest location number is {lowest_loc} for seed {output["seeds"][output["locations"].index(lowest_loc)]}')
if __name__ == "__main__":
    run()