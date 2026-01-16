import os
from subprocess import call

THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
PREAMBLE = os.path.join(THIS_FILE_DIR, 'preamble.c')
EPILOGUE = os.path.join(THIS_FILE_DIR, 'epilogue.c')

class LabelScanner(object):
    def __init__(self):
        self.labels_needed = set()

    def get_label(self, code_line):
        self.labels_needed.add(code_line)
        return 'NULL'

class Labeler(object):
    def get_label(self, code_line):
        return 'lbl_%d' % code_line

def compilecode(program, out_filename):
    # Convert program to C code
    midfile = out_filename+'.mid.tmp'
    with open(midfile, 'w') as outf:
        translate(program, outf)

    # Paste together preamble/C code/epilogue
    c_file = out_filename+'.c'
    with open(c_file, 'w') as outf:
        with open(PREAMBLE, 'r') as infile:
            outf.write(infile.read())
        with open(midfile, 'r') as infile:
            outf.write(infile.read())
        with open(EPILOGUE, 'r') as infile:
            outf.write(infile.read())
    os.remove(midfile)

    # Compile whole C program
    call(['gcc', '-O1', '-o', out_filename, c_file])
    os.remove(c_file)
     
def translate(program, out_stream):
    labeler = LabelScanner()
    # 1st pass - figure out what labels are needed
    for inst in program:
        inst.to_c(labeler)

    # 2nd pass - fill in code
    labels_needed = labeler.labels_needed
    labeler = Labeler()
    for idx in range(len(program)):
        if idx in labels_needed:
            out_stream.write('lbl_%d:\n' % idx)
        out_stream.write(program[idx].to_c(labeler)+';\n')
