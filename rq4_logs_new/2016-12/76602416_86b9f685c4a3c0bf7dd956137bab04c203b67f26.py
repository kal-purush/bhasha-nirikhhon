import csv

def parseTrainingCSV(trainingSetFile,noOfTrainingExamples):
    data = []
    h=[]
    t=[]
    #csv_data = csv.reader(file("C:\\D-Drive\\studies\\MCS\\SML\\Project\\yelp_data\\yelp_training_set\\training_data_dummy.csv"))
    f=open(trainingSetFile,"r")
    csv_data = csv.reader(f) #file(sys.argv[1])
    i=0
    for row in csv_data:
        i=i+1
        if(i==1): #First line containing the names of the features
            continue
        if(i>noOfTrainingExamples+1):#Number of training examples say 500
            break
        data.append(row)
    for i in range(0,len(data)):
        features=[]
        for j in range(1,len(data[i])-1):
            #h.append(data[i][j])
            features.append(float(data[i][j]))
        h.append(features)
        t.append(float(data[i][len(data[i])-1]))
    f.close()
    return (h,t)

def parseTestingCSV(testingSetFile,startLine,noOfExamples):
    data = []
    h=[]
    t=[]
    #csv_data = csv.reader(file("C:\\D-Drive\\studies\\MCS\\SML\\Project\\yelp_data\\yelp_training_set\\training_data_dummy.csv"))
    f=open(testingSetFile,"r")
    csv_data = csv.reader(f) #file(sys.argv[1])
    i=0
    for row in csv_data:
        i=i+1
        if(i<startLine):
            continue
        if(i>startLine+noOfExamples):
            break
        data.append(row)
    for i in range(0,len(data)):
        features=[]
        for j in range(1,len(data[i])-1):
            #h.append(data[i][j])
            features.append(float(data[i][j]))
        h.append(features)
        t.append(float(data[i][len(data[i])-1]))
    f.close()
    return (h,t)