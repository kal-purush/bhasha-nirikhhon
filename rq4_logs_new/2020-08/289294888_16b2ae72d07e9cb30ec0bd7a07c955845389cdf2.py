##supportive functions to create the web application.
import json, os, pprint

response = []

def list_cat_recipes(cat):
    cat_recipes = []
    allfiles = os.listdir('/home/sabeer/TIVA/web/json')
    for rfile in allfiles:
        cat_filename = rfile.split('-')[0]
        if cat_filename == cat:
            #print('cat_filename:', rfile)
            with open('/home/sabeer/TIVA/web/json/%s' % rfile) as f:
                recipe_data = json.load(f)
                cat_recipe_name = recipe_data['Name']
            #print('The recipe %s comes under the category %s.' % (cat_recipe_name,cat))
            cat_recipes.append(cat_recipe_name)
    return cat_recipes

def list_categories():
    category = []
    i = 0
    json_files = os.listdir('/home/sabeer/TIVA/web/json')
    for each_file in json_files:
        cat_name = each_file.split('-')[0]
        if cat_name not in category:
            category.append(cat_name)
    return category

'''
def print_recipes():
    response = list_all_names()
    count = 0
    for ele in response:
        print(ele, end='\t')
        #print(ele),
        count = count + 1
        if count % 3 == 0:
            print()
    return response
'''


def list_all_names():
    recipe_names = []
    json_files = os.listdir('/home/sabeer/TIVA/web/json')
    for each_file in json_files:
        with open('/home/sabeer/TIVA/web/json/%s' % each_file) as f:
            data = json.load(f)
        #print(each_file, data)
        name = data['Name']
        #print('receipe name:', name)
        recipe_names.append(name)
    return recipe_names


#print(list_cat_recipes('Tea'))
#print(list_categories())
