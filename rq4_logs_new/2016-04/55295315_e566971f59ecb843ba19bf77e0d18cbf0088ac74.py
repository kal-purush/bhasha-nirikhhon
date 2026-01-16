import re
import sys

interesting_labels = ["_start", "recaller", "main", "foo", "bar"]

label_pattern = re.compile(r"^(\w+) <(\w+)>:$")
instruction_pattern = re.compile(r"\s*\w+:\s*(\w+).*")

jump_exit_pattern = re.compile(r"\s*\w+:\s*\w+\s+j\s+\w+\s+<exit>")

NOP = "00000013"
J_WAT = "0000006f"
file_names = sys.argv[1:]

for filename in file_names:
    with open(filename, 'r') as f:
        with open(filename + '.filtered', 'w') as out_f:
            printing = False
            insert_nops = -1
            nop_count = 0
            for line in f.readlines():
                m = label_pattern.search(line)
                if m:  # this is a label
                    label = m.group(2)
                    if label in interesting_labels:
                        printing = True
                        location_shifted = hex(int(m.group(1), 16) / 4)[2:]
                        line = "@%s\n" % location_shifted
                        if label == "_start":
                            insert_nops = 4

                    if label not in interesting_labels:
                        printing = False
                else:  # Check if this is an instruction line
                    m = instruction_pattern.search(line)
                    if m:
                        j_match = jump_exit_pattern.search(line)
                        if j_match:
                            line = "%s\n" % J_WAT
                        else:
                            line = "%s\n" % m.group(1)

                    else:  # garbage line
                        line = None

                if printing and line:
                    if insert_nops > 0:
                        insert_nops -= 1
                    if insert_nops == 0:
                        insert_nops = -1
                        nop_count = 5
                    if nop_count > 0:
                        line = "%s\n" % NOP
                        nop_count -= 1
                    out_f.write(line)