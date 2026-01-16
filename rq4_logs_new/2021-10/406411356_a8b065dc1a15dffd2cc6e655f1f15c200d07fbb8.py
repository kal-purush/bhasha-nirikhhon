# Напишите комбинацию циклов чтобы проитерировать все элементы сложной структуры 
# данных вида d = {“a”: {“1”: [...], “2”: [...], ...}, ...} (словарь словарей в которых лежат списки) 

d = {
    "a": {
        "1": ['A', 'B', 'C'], 
        "2": ['D', 'E', 'F']
        }, 
    "b": {
        "1": ['$', '@', '#'], 
        "2": ['!', '^', '*', '&']
        }, 
    "c": ['1', '2',]
    }


def iter_data(data):
    for key in data:
        #print(type(data[key]))
        if type(data[key]) is dict:
            print("{}: '/'".format(key))
            iter_data(data[key])
            print("'/'")
        if type(data[key]) is list:
            print("{}: '/'".format(key))
            for item in data[key]:
                print("\t{}".format(item))
            print("'/'")

             
 
iter_data(d)