import os, sys

result_dir = sys.argv[1]
output_file= sys.argv[2]

def main():
    benchmark_set = ['astar', 'bzip2', 'gcc', 'libquantum', 'mcf', 'omnetpp',
            'perlbench', 'soplex', 'sphinx3', 'xalancbmk']

    all_lines = []
    prefetcher = []
    for filename in os.listdir(result_dir):
        if filename.find("perf") != -1:
            f = open(result_dir + '/' + filename)
            lines = f.readlines()
            all_lines.append(lines)
            prefetcher.append(filename.split('.')[0])
            f.close()

    num_files = len(all_lines)
    assert(num_files >= 0)

    i = 0
    j = 0
    outstr = ''
    for i in range(0, len(all_lines[0])):
        lines = []
        for j in range(num_files): 
            lines.append(all_lines[j][i])

        if len(lines[0]) == 1 and lines[0][0] == '\n':
            outstr += '\n'
            continue
        elif lines[0].find("##") != -1:
            outstr += "## " + lines[0].split(' ')[-1]
            outstr += "benchmark"
            for j in range(num_files):
                #outstr += ', ' + prefetcher[j]
                outstr += '\t' + prefetcher[j]
        else:
            outstr += lines[0].split(' ')[0]
            for j in range(num_files):
                outstr += '\t' + lines[j].split(' ')[-1][0:-1]
        outstr += '\n'

    f = open(output_file, 'w')
    f.write(outstr)
    f.close()

    #print outstr

if __name__ == "__main__":
    main()