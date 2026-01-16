ref_seq = ""
acc = acc_cal(ref_seq)
header = build_occ_header(ref_seq, )


input_reads = [] # list of strings

def wc_comp(seq):
    compliment = {'A': 'T', 'C': 'G', 'G': 'C', 'T': 'A'}
    return ''.join([compliment[seq[len(seq) - i -1]] for i in range(len(seq))])

def acc_cal(seq):
    acc = {'A': 0, 'C': 0, 'G': 0, 'T': 0}
    for i in range(seq):
        acc[seq[i]] += 1
    acc['C'] += acc['A']
    acc['G'] += acc['C']
    acc['T'] += acc['G']
    return acc

def build_occ_header(seq, header_size):
    header = [[{'A': 0, 'C': 0, 'G': 0, 'T': 0}]] # list of dictionaries
    for i in range(1, len(seq)/header_size):
        header[i] = header[i-1]
        for j in range(header_size):
            header[i][seq[i*header_size+j]] += 1
    return header

def occ_cal(seq, header, header_size):
    pass

def occur(base, idx):
    occ_cal(seq, header, header_size)

def suffix_array():
    pass

# Seed generation
def smem_gen(read, init_pos):
    # backward(forward) extension
    # Ik(xd) = acc[x] + occur(x, Ik(d) − 1)
    # Il(xd) = acc[x] + occur(x, Il(d)) − 1

    # Output: A set of SMEMs that contains the character in init_pos.
    return

# Seed extension
def sw_align():
    pass
