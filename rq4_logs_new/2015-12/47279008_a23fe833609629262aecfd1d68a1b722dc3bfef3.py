import re
import fileinput

first = re.compile(r'.*(.).\1.*')
second = re.compile(r'.*(.{2}).*\1.*')

for line in fileinput.input():
   line = line.rstrip() 
   if first.match(line) and second.match(line):
      print line