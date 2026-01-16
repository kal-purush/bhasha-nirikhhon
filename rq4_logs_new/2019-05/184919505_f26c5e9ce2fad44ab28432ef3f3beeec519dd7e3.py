import os

# # get the current directory
# cur_dir = os.path.dirname(os.path.abspath(__file__))

# start removing the unneeded files
os.remove('./public/favicon.ico')
os.remove('./src/App.css')
os.remove('./src/App.test.js')
os.remove('./src/logo.svg')
os.remove('./src/serviceWorker.js')

# start removing the unneeded lines in index.html
with open('./public/index.html', 'r') as infile:
    lines = infile.readlines()

with open('./public/index.html', 'w') as outfile:
    for pos, line in enumerate(lines):
        if pos != 5:
            outfile.write(line)