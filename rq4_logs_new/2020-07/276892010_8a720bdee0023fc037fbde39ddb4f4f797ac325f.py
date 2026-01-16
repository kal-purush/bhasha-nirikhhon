
def MakeAcronym(str):
    n_str = ""
    for it in range(len(str)):
        if it == 0:
            n_str+=str[it]
            if str[it] == "З":
                n_str+=str[it+1]+str[it+2]
            continue

        if (str[it-1] == ' ' or str[it-1] == '-') and (str[it] != ' ' or str[it] != '-'):
            if str[it-1] == '-':
                n_str+=str[it-1]
            n_str+=str[it].upper()
    return n_str

class JsonList(list):
    def __init__(self, collection):
        self.collection = collection

    def GetAllUniquWalue(self, param_name):
        qnique_collection = list()
        for it in self.collection:
            val = it.get(param_name)
            if val != None and qnique_collection.count(val) == 0:
                qnique_collection.append(val)
        return qnique_collection

    def Count(self):
        return len(self.collection)
    
    def GetEqualRowsCount(self, param_name, collection):
        n_collection = list()
        counter = 0
        for it in collection:           
            for it2 in self.collection:
                val = it2.get(param_name)
                if val != None:
                    if type(val) == type([]):
                        if val[0] == it:
                            counter+=1
                    elif val == it:
                        counter+=1
            n_collection.append([it, counter])
            counter = 0
        return JsonList(n_collection)

    def ZipFilds(self, collection, name_1 = 0, name_2 = 0, name_3 = 0):
        n_collection = list()
        for it in self.collection:
            for it2 in collection.collection:
                if it[0] == it2[0]:
                    n_collection.append({name_1:it[0], name_2:it[1], name_3:it2[1]})
                    break
        return JsonList(n_collection)

    def MakePairComparisonData(self, name = 0, value1 = 1, value2 = 2):
        data = list()
        data.append([])
        data.append([])
        data.append([])
        for it in self.collection:
            n_name = MakeAcronym(it[name])
            data[0].append(n_name)
            data[1].append(it[value1])
            data[2].append(it[value2])
        return JsonList(data)

    def MakeComparisonData(self, name = 0, value = 1):
        data = list()
        data.append([])
        data.append([])
        for it in self.collection:
            n_name = MakeAcronym(it[name])
            data[0].append(n_name)
            data[1].append(it[value])
        return JsonList(data)

    def GetWereEqual(self, name = 0, value = 0):
        n_collection = list()
        for it in self.collection:
            if it[name] == value:
                n_collection.append(it)
        return JsonList(n_collection)
            

    def __str__(self):
        return self.collection.__str__()

    def GetList(self):
        return self.collection