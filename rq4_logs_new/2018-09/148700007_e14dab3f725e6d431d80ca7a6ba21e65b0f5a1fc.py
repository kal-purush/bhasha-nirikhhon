import os
directory = os.getcwd()
files = []
originals = []
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith(".mkv") or filename.endswith(".mp4") or filename.endswith(".avi"):
            originals.append(filename)
            index = 0
            found = 0
            i = 0
            for ch in file:
                if found != 0:
                    if found == 4:
                        index = i
                        out_str = file[:index].replace(".", " ").replace("(", "").replace(")", "")
                        out_str = out_str[:len(out_str)-5] + " (" + out_str[len(out_str)-4:].replace("  ", " ")
                        out_str += ")" + file[len(file)-4:]
                        files.append(out_str)
                        break
                if ch.isdigit():
                    found += 1
                elif found != 0:
                    found = 0
                i += 1
print("\nDouble checking the list for perfection...\n")
nono = []
caps = []
w = ""
for word in open("nono_words").read():
    if word == "\n":
        nono.append(w)
        w = ""
    else:
        w += word
for word in open("full_caps").read():
    if word == "\n":
        caps.append(w)
        w = ""
    else:
        w += word

    caps.append(word)
for file in files:
    file = file.replace("[", "").replace("]", "")
    n_words = ""
    for word in file.split():
        if n_words != "":
            if word not in nono and word not in caps:
                word = word.capitalize()
            elif word in caps:
                word.upper()
        else:
            word = word.capitalize()
        if n_words != "":
            n_words += " " + word
        else:
            n_words += word
    file = n_words
    print(file)

response = input("\nDoes this list look correct? [Y] / [N]\n")
if response == "Y":
    # do the changes
    print("Performing requested changes... This will only take a few moments.")
    index = 0
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename in originals:
            os.rename(file, originals[originals.index(filename)])
            index += 1
else:
    print("Please try running the program again if you ran into an issue."
          "\nOr try removing problem files if the error persists.")