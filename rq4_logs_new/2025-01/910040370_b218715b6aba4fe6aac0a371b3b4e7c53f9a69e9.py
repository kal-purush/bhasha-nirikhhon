#!/usr/bin/env python3

import os

def process_concatenation_instructions():
    for line in open('concat.txt', 'r'):
        files = line.strip().split()
        if not files:
            continue
        destination = files[-1]
        sources = files[:-1]
        if len(sources) == 0:
            sources = [destination]
        print(f"""{sources} -> {destination}""")
        with open(os.path.join("xsb-concat", destination + ".xsb"), 'w') as outfile:
            for source in sources:
                try:
                    with open(os.path.join("xsb", source + ".xsb"), 'r') as infile:
                        outfile.write(infile.read())
                        # Add a newline between files to prevent content from running together
                        outfile.write('\n')
                except FileNotFoundError:
                    print(f"Warning: Source file '{source}' not found, skipping...")

if __name__ == "__main__":
    process_concatenation_instructions()