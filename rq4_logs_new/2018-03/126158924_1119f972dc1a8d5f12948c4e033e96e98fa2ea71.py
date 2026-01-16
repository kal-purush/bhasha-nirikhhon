#!/usr/bin/env python3
from os import listdir
from os.path import isfile, join
import re
import csv
import pdfkit
import os
import pypandoc
import sys

import configparser
import io

sample_config = """
[questions]
module = CS126
QN = Problem
Q1 = Iterator
Q2 = Patient Sorting
Q3 = Virus Sequence Set
Q4 = ID HashMap
Q5.1 = Patients Per Calendar Week
Q5.2 = Patient Surname Searching
"""
config = configparser.RawConfigParser(allow_no_value=True)
config.read_string(sample_config)


def outputm(value):
    return '<span style="color:red">[' + value + "]</span>"


def outputt(line):
    # line = re.sub(r'`(.*?)`',r'<code>\1</code>', line.rstrip())
    return line


def qtotal(q, marks, avails):
    return outputm("Total Q" + str(q) + " " + str(marks[q]) + "/" + str(avails[q]))


def append_header(lines):
    lines.append("---")
    lines.append("papersize: 'a4'")
    lines.append('geometry: "left=3cm, right=3cm, top=2cm, bottom=2cm"')
    # change marking color here, see: https://en.wikibooks.org/wiki/LaTeX/Colors
    lines.append("linkcolor: 'RubineRed'")
    lines.append("---")


def append_question_title(question, total, max_avail):
    return "\n## " + config.get("questions", "QN") +" " + question +" - " + config.get("questions", "Q" + question) + " " +outputm( str(total) + "/" + str(max_avail) ) + ""

if __name__ == "__main__":
    mypath = "return/"
    outdir = "pdfs/"
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f)) and f.endswith(".txt")]
    onlyfiles.sort()
    expected = 100

    nolatex = len(sys.argv) > 1 and sys.argv[1] == "nolatex" 


    cropped = len(sys.argv) > (2 if nolatex else 1)
    if cropped:
        onlyfiles = onlyfiles[-int(sys.argv[1]):]

    if os.path.isfile('results.csv'):
        os.remove('results.csv')

    with open('results.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["UniID", "Mark"])
        if cropped:
            writer.writerow(["Warning", "Only some files here, rerun without limit"])
            
        for f in onlyfiles:
            with open(mypath + f,"r") as g:
                uid = f.split(".")[0]
                q = -1
                marks = {}
                avails = {}
                comments = []
                badmarks = []
                penalties = []
                current_title_location = -1
                ignore = False

                append_header(comments)
                comments.append("# " + config.get("questions","module") + " Assignment Feedback - " + str(uid[1:]))

                for line in g:
                    print(line)
                    if line.startswith("%"):
                        pass
                    if line.startswith("x"):
                        ignore = True
                    if line.startswith("# "):
                        ignore = False
                        qs = re.search('# ([\d|\.]*)( {\d*}{\d*})?', line)
                        newq = (qs.group(1))

                        if q !=-1:
                            comments[current_title_location] = append_question_title(q, marks[q], avails[q])

                        q = newq
                        comments.append("<h2>Question " + str(q) + "</h2>")
                        current_title_location = len(comments)-1
                        print(q)
                        marks[q] = 0
                        avails[q] = 0
                        comment =  line[len(qs.group(0)):].strip()
                        if len(comment) >0 :
                            comments.append(outputt(comment))

                    m = re.search('{(\d*)}{(\d*)}', line)
                    if m != None and not ignore:
                        mark = m.group(1)
                        if mark == "":
                            mark = "0"
                        marks[q] += int(mark)
                        avail = m.group(2)
                        avails[q] += int(avail)
                        assert(int(mark) <= int (avail))

                    if line.startswith("## ") and not ignore:
                        m = re.search('## (.*) {(\d*)}{(\d*)}', line)
                        comment =  line[len(m.group(0)):].strip()
                        if len(comment) > 0 :
                            # appends subquestion title and its comment, a becomes a) in bold.
                            # needs a newline afterwards, otherwise it will all bunch up in a continuous line
                            comments.append("**"+ m.group(1).strip() + "**  " + comment)
                            comments.append("\n")
                    if line.startswith("###") and not ignore:
                        comments.append(outputt(line[3:].strip()))

                    if line.startswith("#P"):
                        m = re.search('#P {(-\d*)}', line)
                        penalties.append((int(m.group(1)),line[len(m.group(0)):].strip() ))

                # Inserts the final question title total, either 5.1 or 5.2
                comments[current_title_location] = append_question_title(q, marks[q], avails[q])
                totalm = sum([marks[a] for a in marks])
                totala = sum([avails[a] for a in avails])
                print(totalm)
                print(totala)
                assert(totala == expected)
                if len(penalties) > 0:
                    count_penalties = 0
                    loss_total = 0
                    comments.append("### Total before penalties: " +outputm( str(totalm) + "/" + str(totala) ) + "\n")

                    for (loss, why) in penalties:
                        comments.append("* "+ why + " [" + str(loss) + "]")
                        count_penalties += 1
                        loss_total += loss
                        totalm += loss
                    # insert penalty title line before the list of penalties
                    comments.insert(len(comments) - count_penalties, "\n## Penalties: " + "" +outputm( str(loss_total) ) + "")


                comments.append("\n## Grand Total: " + "" +outputm( str(totalm) + "/" + str(totala) ) + "")

            writer.writerow([uid[1:], totalm])
            print("------------------Current ID: {} with {} marks------------------".format(uid[1:], totalm))
            #print(marks)
            comments_on_newlines = "\n".join([comment for comment in comments])
            print(comments_on_newlines)

            outfile = open('temp.md', 'w')
            outfile.write(comments_on_newlines)
            outfile.close()

            output_filename =  outdir + uid[1:] 

            if nolatex:
                final_pdf_output = pypandoc.convert_file('temp.md', "html", outputfile=output_filename + ".pdf", extra_args=["--pdf-engine=weasyprint"])
                assert final_pdf_output == ""
            else:
                final_pdf_output = pypandoc.convert_file('temp.md', "pdf", outputfile=output_filename + ".pdf")
                assert final_pdf_output == ""
               


            os.remove('temp.md')

