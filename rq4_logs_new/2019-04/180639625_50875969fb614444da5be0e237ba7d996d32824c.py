#!/usr/bin/env python7

import tkinter as tk
import re
from modules import ParsingModule as PM
from modules import IterationModule as IM

import os
import gettext     
root= os.getcwd()
t = gettext.translation('translate', root, languages=['en'])
_= t.gettext
t.install()


CONST_ALPHABET = ['a', 'b', 'c', '*']

PROJECT_NAME_WIDTH = 50
TEXT_WIDTH = 50
TEXT_HEIGHT = 25
LOG_HEIGHT = 5
LOG_WIDTH = 100

PADX = 10
PADY = 10
PADY_FIRST = 50
WORD_WIDTH = 15
INPUT_WIDTH = 20

CONST_ITERATION = 1000

root = tk.Tk()
save_rules = False

Errors = {
    "empty": _("Epmty input word, input word in area: \"Input word\"\n"),
    "done": _("Algorithm was done successfully\n"),
    "cycle": _("Algorithm was cycled, check errors in your algorithm\n"),
    "arrow_not_found": _("Missing arrow in rule:\n"),
    "str_number_err": _("Error in rule number - %i \n"),
    "not_in_alphabet": _("Sumbol \"{0}\" not from alphabet. Error in index - {1}\n")}


def write_logs(string):
    text_logs.config(state=tk.NORMAL)
    text_logs.delete(1.0, tk.END)
    text_logs.insert(1.0, string)
    text_logs.config(state=tk.DISABLED)

# str rule
# int raw
# return lst|bool
def checkRules(rule, raw):
    rule_lst = PM.ruleStrToList(rule)
    if not rule_lst:
        help_str = Errors["arrow_not_found"] + rule + "\n"
        write_logs(Errors["str_number_err"] % raw + help_str)
        return False
    rule_val = PM.validateRuleList(rule_lst)
    if not rule_val:
        return rule_lst
    i = rule_val[0]
    k = rule_val[1]
    help_str = Errors["not_in_alphabet"].format(k, i)
    write_logs(Errors["str_number_err"] % raw + help_str)
    text_algorithm.tag_add("Error", str(raw) + "." + str(i))
    text_algorithm.tag_config("Error", foreground="red")
    return False


def parseRules(rules):
    i = 1
    lst = []
    for rule in rules:
        rule = rule.rstrip()
        if (rule == ''):
            continue
        parsed_rule = checkRules(rule, i)
        if not parsed_rule:
            return False
        else:
            lst.append(parsed_rule)
        i += 1
    return lst

def uploadRawRules(rules):
    listbox.delete(0, tk.END)
    i = 1
    for rule in rules:
        rule = rule.rstrip()
        if rule == '':
            continue
        listbox.insert(tk.END, ("(%i) " % i) + rule)
        i += 1


def startMarkov(event):
    if save_rules:
        return
    if (input_.get() == ''):
        write_logs(Errors["empty"])
        return
    uploadRawRules(text_algorithm.get(1.0, tk.END).split("\n"))
    rules = parseRules(text_algorithm.get(1.0, tk.END).split("\n"))
    if not rules:
        return
    uploadRules(rules)
    sinput = input_.get()
    i = 0
    while(True):
        if (i >= CONST_ITERATION):
            write_logs(Errors["cycle"])
            result.set('')
            return
        iter_res = IM.doIteration(rules, sinput)
        sinput = iter_res[0]
        if (iter_res[1]):
            break
        i += 1
    result.set(sinput)
    write_logs(Errors["done"])


# return bool
def initSteps():
    if (input_.get() == ''):
        write_logs(Errors["empty"])
        return False
    global save_rules
    save_rules = parseRules(text_algorithm.get(1.0, tk.END).split("\n"))
    if not save_rules:
        return False
    uploadRules(save_rules)
    button_start.config(state=tk.DISABLED)
    textbox_input_word.config(state=tk.DISABLED)
    text_algorithm.config(state=tk.DISABLED)
    result.set(input_.get())
    return True


# void
def uploadRules(rules):
    listbox.delete(0, tk.END)
    for index in range(len(rules)):
        if (rules[index][2]):
            arrow = "  \u21A6  "
        else:
            arrow = "  \u2192  "
        rule_number = '   (' + str(index + 1) + ')   '
        rule_view = rules[index][0] + arrow + rules[index][1]
        listbox.insert(tk.END, rule_number + rule_view)


def endStep():
    global save_rules
    save_rules = False
    button_start.config(state=tk.NORMAL)
    textbox_input_word.config(state=tk.NORMAL)
    text_algorithm.config(state=tk.NORMAL)


def stepMarkov(event):
    if not save_rules:
        if not initSteps():
            return
    iter_res = IM.doIteration(save_rules, result.get())
    result.set(iter_res[0])
    if (iter_res[2] < listbox.size()):
        for i in range(0, listbox.size()):
            listbox.itemconfig(i, bg="white", fg="dark slate gray")
        listbox.itemconfig(iter_res[2], bg="SlateBlue4", fg="snow")
    if (iter_res[1]):
        endStep()
        write_logs(Errors["done"])
        return


def stopMarkov(*args):
    endStep()
    result.set('')


def inputWord(act, inp):
    if (act == '0'):
        return True
    if (inp in CONST_ALPHABET):
        return True
    return False


def select(event):
    print(222)
    print(listbox_lng.curselection())

inputWord_reg = (root.register(inputWord), "%d", "%S")

root.title = "Markov"
root.columnconfigure(0, weight=1)
root.rowconfigure(0, weight=1)

result = tk.StringVar()

input_ = tk.StringVar()
input_.set("aaaaa")

# ------------------------LABELS--------------------
name = _("Normal markov algorithm")
label_project_name = tk.Label(root, text=name, width=PROJECT_NAME_WIDTH)
label_project_name.configure(fg="deep pink")
label_project_name.grid(row=0, column=2, columnspan=4, pady=PADY_FIRST)

label_input_word = tk.Label(root, text=_("Input word:"), width=WORD_WIDTH)
label_input_word.configure(fg="midnight blue")
label_input_word.grid(row=1, column=0, columnspan=2, sticky=tk.N + tk.S)

label_output_word = tk.Label(root, text=_("Output word:"), width=WORD_WIDTH)
label_output_word.configure(fg="midnight blue")
label_output_word.grid(row=1, column=6, columnspan=2, sticky=tk.N + tk.S)

label_simbols = tk.Label(root, text=_("Arows symbols:"))
label_simbols.configure(fg="midnight blue")
label_simbols.grid(row=1, column=2, columnspan=4, sticky=tk.S)

info_str = " \u21A6 means \"|->\"   \u2192  means \"->\" "
label_arrow = tk.Label(root, text=info_str)
label_arrow.configure(fg="midnight blue")
label_arrow.grid(row=2, column=2, columnspan=4, sticky=tk.N)

label_alphabet = tk.Label(root, text=_("Alphabet:"), width=10)
label_alphabet.configure(fg="midnight blue")
label_alphabet.grid(row=3, column=2, columnspan=1, pady=PADY_FIRST, sticky=tk.E)

label_rule = tk.Label(root, text=_("Rules:"), width=WORD_WIDTH)
label_rule.configure(fg="midnight blue")
label_rule.grid(row=4, column=0, columnspan=2, sticky=tk.S + tk.W)

label_exec = tk.Label(root, text=_("Steps:"), width=WORD_WIDTH)
label_exec.configure(fg="midnight blue")
label_exec.grid(row=4, column=4, columnspan=2, sticky=tk.S + tk.W, padx=20)

# ------------------------TEXT-----------------
textbox_input_word = tk.Entry(root, width=INPUT_WIDTH, textvariable=input_)
textbox_input_word.grid(row=2, column=0, columnspan=2, padx=PADX, sticky=tk.S)
textbox_input_word.config(validate="key", validatecommand=inputWord_reg)

textbox_output_word = tk.Entry(root, width=INPUT_WIDTH, textvariable=result)
textbox_output_word.config(state=tk.DISABLED)
textbox_output_word.grid(row=2, column=6, columnspan=2, padx=PADX, sticky=tk.S)

textbox_alphabet = tk.Text(root, width=20, height=1)
textbox_alphabet.insert(1.0, "abc*")
textbox_alphabet.config(state=tk.DISABLED)
textbox_alphabet.grid(row=3, column=3, columnspan=2, pady=PADY, sticky=tk.W)

text_algorithm = tk.Text(root, width=TEXT_WIDTH, height=TEXT_HEIGHT, wrap=tk.WORD)
text_algorithm.grid(row=5, column=0, columnspan=4, sticky=tk.W, padx=PADX)
file = open("test.txt", "r")
algorithm_rules = file.read()
text_algorithm.insert(0.0, algorithm_rules)

text_logs = tk.Text(width=LOG_WIDTH, height=LOG_HEIGHT)
text_logs.grid(row=7, column=0, columnspan=8, padx=PADX, pady=PADY)
text_logs.config(state=tk.DISABLED)

# ------------------------LISTBOX------------------
listbox = tk.Listbox(root, width=TEXT_WIDTH-2, height=TEXT_HEIGHT-1)
listbox.grid(row=5, column=4, columnspan=4, sticky=tk.E, padx=PADX)

listbox_lng = tk.Listbox(root)
listbox_lng.grid(row=3, column=6, columnspan=2)
listbox_lng.insert(tk.END, "English")
listbox_lng.insert(tk.END, "Russian")
listbox_lng.bind("<<LisboxSelect>>", select)

# ----------------------BUTTONS-----------------------
button_start = tk.Button(root, text=_("Start"), width=15)
button_start.configure(bg="green", fg="yellow")
button_start.grid(row=6, column=0, columnspan=2, padx=PADX, pady=PADY)
button_start.bind("<Button-1>", startMarkov)

button_stop = tk.Button(root, text=_("Stop"), width=15)
button_stop.configure(bg="red", fg="white")
button_stop.grid(row=6, column=2, columnspan=2, padx=PADX, pady=PADY)
button_stop.bind("<Button-1>", stopMarkov)

button_step = tk.Button(root, text=_("Step"), width=15)
button_step.configure(bg="MediumPurple2", fg="antique white")
button_step.grid(row=6, column=4, columnspan=2, padx=PADX, pady=PADY)
button_step.bind("<Button-1>", stepMarkov)

button_exit = tk.Button(root, text=_("Exit"), width=15, command=root.quit)
button_exit.configure(bg="antique white", fg="red")
button_exit.grid(row=6, column=6, columnspan=2, padx=PADX, pady=PADY)

root.mainloop()