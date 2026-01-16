from prettytable import PrettyTable
import calendar
import string

def printIndTable():
    x = PrettyTable(field_names=['ID','Name','Gender','Birthday','Age','Alive','Death','Child','Spouse'])
    x.align['ID'] = 'l'
    x.padding_width = 1
    with open('project03.ged') as f:
        list1 = []
        for idx,line in enumerate(f):
            a = line.strip()
            b = a.split(' ')
            list1.append(b)

        for idx, item in enumerate(list1):

            if len(item) > 2 and item[2] == 'INDI':
                id = item[1]

                # print(id)

                Name = ''
                if list1[idx+1][1] == 'NAME':
                    for i in list1[idx + 1][2:]:
                        Name += str(i)
                else:
                    Name = 'NA'
                # print(Name)
                if list1[idx+5][1] == 'SEX':

                    Gender = list1[idx + 5][2]
                else:
                    Gender = 'NA'

                # print(Gender)

                Birthday = list1[idx + 7][4] + '-' + str(list(calendar.month_abbr).index(string.capwords(list1[idx + 7][3]))) + '-' + \
                           list1[idx + 7][2]

                # print(Birthday)
                if list1[idx + 8][1] == 'DEAT':
                    Age = int(list1[idx + 9][4])-int(list1[idx + 7][4])
                else:
                    Age = 2019 - int(list1[idx + 7][4])

                # print(Age)

                if list1[idx + 8][1] == 'DEAT':
                    Alive = False
                    Death = list1[idx + 9][4] + '-' + str(
                        list(calendar.month_abbr).index(string.capwords(list1[idx + 9][3]))) + '-' + list1[idx + 9][2]
                else:
                    Alive = True
                    Death = 'NA'


                list2 = []
                spouse = ''
                famid = ''
                if list1[idx + 8][1] == 'DEAT':
                    if list1[idx + 10][1] == 'FAMS':
                        famid += list1[idx + 10][2]

                elif list1[idx + 8][1] == 'FAMS':
                    famid += list1[idx + 8][2]
                    # print(famid)

                for index, items in enumerate(list1):
                    if items[1] == famid:
                        Index = index+3
                        while list1[Index][1]=='CHIL':
                            list2.append(list1[Index][2].strip('@'))
                            Index += 1

                Child = ''
                if list2 != []:
                    Child = set(list2)

                else:
                    Child = 'NA'

                spouse = []
                for index, items in enumerate(list1):
                    if items[1] == famid:
                        if list1[index+1][2] == id:
                            spouse.append(list1[index+2][2].strip('@'))
                        if list1[index+2][2] == id:
                            spouse.append(list1[index+1][2].strip('@'))
                if spouse != []:
                    Spouse = set(spouse)
                else:
                    Spouse = 'NA'

                x.add_row([id.strip('@'), Name, Gender, Birthday, Age, Alive, Death, Child, Spouse])

    return x

def printFamTable():
    x = PrettyTable(field_names=['ID', 'Married', 'Divorced', 'Husband ID', 'Husband Name', 'Wife ID', 'Wife Name', 'Children'])
    x.align['ID'] = 'l'
    x.padding_width = 1
    with open('project03.ged') as f:
        list1 = []
        for idx,line in enumerate(f):
            a = line.strip()
            b = a.split(' ')
            list1.append(b)

        for idx,item in enumerate(list1):
            ID = ''
            if len(item)>2 and item[2] == 'FAM':
                ID = item[1]

            for index,items in enumerate(list1):
                Married = ''
                Divorced = ''
                if items[1] == ID:
                    Index = index+3
                    while list1[Index][1] != 'MARR':
                        Index += 1
                    Married = list1[Index + 1][4] + '-' + str(
                        list(calendar.month_abbr).index(string.capwords(list1[Index+1][3]))) + '-' + \
                               list1[Index+1][2]
                    if list1[Index+2][1] == 'DIV':
                        Divorced = list1[Index + 3][4] + '-' + str(
                            list(calendar.month_abbr).index(string.capwords(list1[Index + 3][3]))) + '-' + \
                                  list1[Index + 3][2]
                    else:
                        Divorced ='NA'
                    HusbandID = list1[index+1][2]
                    WifeID = list1[index+2][2]
                    HubbandName = ''
                    for num,thing in enumerate(list1):
                        if thing[1]==HusbandID:
                            for i in list1[num + 1][2:]:
                                HubbandName += str(i)
                    WifeName = ''
                    for num, thing in enumerate(list1):
                        if thing[1] == WifeID:
                            for i in list1[num + 1][2:]:
                                WifeName += str(i)
                    list2 = []
                    for num, thing in enumerate(list1):
                        if thing[1] == ID:
                            Num = num + 3
                            while list1[Num][1] == 'CHIL':
                                list2.append(list1[Num][2].strip('@'))
                                Num += 1


                    if list2 != []:
                        Child = set(list2)

                    else:
                        Child = 'NA'
                    x.add_row([ID.strip('@'),Married,Divorced,HusbandID.strip('@'),HubbandName,WifeID.strip('@'),WifeName,Child])
    return x

if __name__ == '__main__':
    a = printIndTable()
    b = printFamTable()
    print(a)
    print(b)
    with open('output.txt', 'w') as file:
        file.write(str(a))
        file.write(str(b))