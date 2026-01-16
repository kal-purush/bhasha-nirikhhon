import sys

data_dir = sys.argv[1]
seq_length = int(sys.argv[2])
print "Preparing input for data %s with length %d" % (data_dir, seq_length)

in_file  = open("data/%s/raw_input.txt" % data_dir, 'r')
out_file = open("data/%s/input.txt" % data_dir, 'w')

for line in in_file:
    n_seq = len(line) - seq_length + 1
    if n_seq <= 0:
        continue
    line = '.' + line[:-1] + '.'
    for s in range(0, n_seq):
        out_file.write("%s %s\n" % (line[s : s+seq_length],
                                    line[s+1 : s+seq_length+1]))
