
"""
Parses a canvas Peer Review page and collates assignments that were submitted.

Each student will recieve a copy of their own assignment + the critique they wrote,
plus anonymous copies of the critiques written about their assignment.
"""

import sys, random, subprocess, os, re
from collections import defaultdict, OrderedDict
from glob import glob

from bs4 import BeautifulSoup

do_it = True

if len(sys.argv) < 2:
    print "no file specified!"
    sys.exit(1)

page = BeautifulSoup(open(sys.argv[1]), "html.parser")

# do_it = False
# if '--go' in sys.argv:
#     print "doing it"
#     do_it = True
# else:
#     print "dry-run only, pass '--go' to actually make the files"

"""a dict from id -> id giving the mapping of who each student was assigned to review"""
reviews = OrderedDict()

"""a dict from id -> [ids] listing the students who reviewed a given student"""
reviews_for = defaultdict(list)

"""a dict from id -> string giving the names for each student"""
names = OrderedDict()


for student in page.select(".student_reviews"):
    user_id = student.find(class_="student_review_id").text
    assigned_ids = student.select(".peer_review .user_id")
    reviews[user_id] = assigned_ids[0].text if assigned_ids else None
    for i in assigned_ids:
        reviews_for[i.text].append(user_id)
    name = student.find(class_="assessor_name").text.split(", ")
    names[user_id] = " ".join([name[-1]] + name[:-1])


assign_re = re.compile(r'Assignment \d+', re.I)
page_title = page.find(string=assign_re)
assignment = ""
if page_title:
    assignment = assign_re.search(page_title).group(0)


def get_files(folder, id):
    return glob("{}/*_{}_*.pdf".format(folder, id))

def prepare_file_args(files, text, fileout, critiques=None):
    args = ["cpdf",
        "-merge"] + files

    args += ["AND",
    "-add-text", text,
    "-topright", "50", "-font", "Courier", "-font-size", "10"]

    if critiques:
        args += ["AND", "-merge"] + critiques

    args += ["-o", fileout]

    return args

file_index = 0
def file_name(filename, type=None):
    global file_index
    filename = filename.split("/")[-1]
    filename = '_'.join(filename.split('_')[:-1])
    file_index += 1
    return "out/{:03}{}_{}.pdf".format(file_index, "_"+type if type else '', filename)


_temp = names.keys()[:]
random.shuffle(_temp)

"""a dictionary mapping id -> number, giving the anonymous essay number of each student"""
anoms = {k:v+1 for v, k in enumerate(_temp)}

if do_it and not os.path.exists("out"):
    os.makedirs("out")

students = [i for i in _temp if reviews[i]]
queued = []
student = None

while students or queued:
    if queued:
        student = queued.pop(0)
    else:
        print
        student = students.pop(0)

    print "{}'s essay [{}]".format(
        names[student], anoms[student])
    if do_it and get_files("submissions", student):
        subprocess.check_call(prepare_file_args(
            files = get_files("submissions", student),
            text = assignment + " Essay: {} [{}]".format(
                        names[student], anoms[student]),
            fileout = file_name(get_files("submissions", student)[0]),
            critiques = [j for i in reviews_for[student] for j in get_files("critiques", i)]
        ))

    for i in reviews_for[student]:
        # print "  {}'s critique of {} [{}]".format(
        #     names[i], names[student], i)
        if get_files("critiques", i):
            print "  {}'s critique of {} [{}]".format(
                names[i], names[student], anoms[student])

            if do_it:
                subprocess.check_call(prepare_file_args(
                    files = get_files("critiques", i),
                    text = assignment + " Critique: {} [{}]".format(
                                names[i], anoms[student]),
                    fileout = file_name(get_files("critiques", i)[0], 'crit')
                ))
        if i in students:
            queued.insert(0, i)
            students.remove(i)
    print

print anoms