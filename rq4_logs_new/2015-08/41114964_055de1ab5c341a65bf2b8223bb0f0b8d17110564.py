__author__ = 'xxy'

import pickle

dic = pickle.load(open('c.pickle','rb'))

wordIdList = [20036, 14280]

wordId1 = wordIdList[0]
wordId2 = wordIdList[1]

docList1 = [(0, 2, [355, 464]), (2, 4, [2, 3, 4, 9]), (4, 3, [10, 29, 56])]
docList2 = [(0, 5, [79, 139, 147, 486, 494]), (2, 6, [10, 23, 26, 29, 33, 36]), (3, 8, [19, 22, 32, 35, 46, 49, 59, 62]), (4, 7, [23, 27, 30, 33, 37, 40, 57])]


print(docList1)
print(docList2)

docId = []

for tuple1 in docList1 :
    docId1 = tuple1[0]
    pos1 = tuple1[2]
    match = 0

    for tuple2 in docList2 :
        if ( tuple2[0] > docId1 ) :
            continue

        # Appears In the Same Doc
        if ( tuple2[0] == docId1 ) :
            pos2 = tuple2[2]
            for i in pos1 :
                if (match == 1) :
                    break
                for j in pos2 :
                    if ((j - i) == 1) :
                        docId.append(docId1)
                        match = 1
                        break
                    if ((j - i) > 1) :
                        break

print(docId)