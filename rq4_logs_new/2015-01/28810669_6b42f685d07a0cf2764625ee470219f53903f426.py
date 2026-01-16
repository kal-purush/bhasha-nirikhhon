import json
import itertools
json_data = open('/notebooks/t3.ipynb')
data = json.load(json_data)
sourcelist = [cell["source"] for cell in data["cells"] if cell["cell_type"] == 'markdown']
allsource = list(itertools.chain(*sourcelist))

template="full"
css="custom.css"
sideleft=""

template_list = [s.partition("=")[2].strip() for s in allsource if s.partition("=")[0] == "template"]
css_list = [s.partition("=")[2].strip() for s in allsource if s.partition("=")[0] == "css"]
sideleft_list = [s.partition("=")[2].strip() for s in allsource if s.partition("=")[0] == "sideleft"]

if len(template_list)>0: template=template_list[-1]
if len(css_list)>0: css=css_list[-1]
if len(css_list)>0: sideleft=sideleft_list[-1]

print(template)
print(css)
print(sideleft)



