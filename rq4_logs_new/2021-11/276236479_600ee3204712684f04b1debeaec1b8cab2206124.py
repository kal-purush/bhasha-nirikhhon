import re
import os

# update the project list locally
def read_txt_file(path, filename):
    full_path = os.path.join(path, filename)
    with open(full_path, 'r') as f:
        txt = f.read()
    f.close()
    return txt

def find_locale_from_prjName(name):
    result = re.search(r"[a-z][a-z]_[A-Z][A-Z]", name)
    if result:
        return result.group(0)

def txt2prjdict(txt):
    prj_list = []
    delimiter = 'http'
    # eliminate all splace and replace last element into overview
    txt = txt.replace(' ', '').replace('\t', '').replace('\xa0', '').replace('stats/ungradedByLocale', 'overview')
    prjs = txt.split('\n')
    for prj in prjs:
        if len(prj) > 0:
            name, link = prj.split(delimiter)
            p_dict = {}
            p_dict['name'] = name
            p_dict['location'] = find_locale_from_prjName(name)
            p_dict['link'] = delimiter + link
            prj_list.append(p_dict)
    print("Number of projects: {}".format(len(prj_list)))
    return prj_list

def get_projectList_from_txt():
    txt = read_txt_file(path="../docs", filename="projects.txt")
    project_list = txt2prjdict(txt)
    return project_list