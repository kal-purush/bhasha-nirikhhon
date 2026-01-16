import re
import datetime
from os import remove

_substitutions = {}

def register_substitution(name: str, value: any):
  _substitutions[name] = value

def clear_substitutions():
  _substitutions.clear()

def renderFile(path: str, templateName: str):
  _substitutions['now'] = datetime.datetime.now().isoformat()
  _substitutions['fileName'] = re.search(r'/?([a-zA-Z_\-\d ]+)$').groups()[0]
  with open(f'templates/{templateName}') as template:
    with open(path, 'wt') as output:
      lineNum = 0
      for line in template:
        ++lineNum
        for name in _substitutions:
          line = re.sub(f'<%{name}%>', _substitutions[name], line)
        has_left = re.search(r'<%([a-zA-Z\d]*)%>', line)
        if has_left != None:
          key = has_left.groups()[0]
          start = has_left.span()[0]
          end = has_left.span()[1]
          output.close()
          remove(path)
          raise NameError(f'Key "{key}" appeared in template but was not given a substitution value [@line {lineNum}: cols {start} -> {end}]')
        output.write(line)