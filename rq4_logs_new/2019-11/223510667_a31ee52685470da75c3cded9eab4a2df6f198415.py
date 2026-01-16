#Andrew Amavizca ME-021
#Project 2        2019
#Iterates through folder of names and calculates the number of occurences
#and popularity for the provided name(in matlab gui).
#The code then formulates an average trendline to be plotted
#and outputs all the data into a textfile to be used by the matlab code. 

import pathlib

#Importing the names folder
path = pathlib.Path('names')

#empty lists to be filled by the data
data = []
t_occur = []

#This function searches for the provided name and sex
#And gathers the data accordingly

def search(name,sex):
    
    #These are here to ensure no errors occur in capatalization
    name = name.lower().capitalize()
    sex = sex.lower().capitalize()

    input_name = name
    input_sex = sex
    i = 0 

    #itterating through the folder and gathering the data
    for entry in path.iterdir():

        #if element in folder is a file
        
        if entry.is_file():                       
            file_name = str(entry)[6:]      #gathers file name              
            year = file_name[3:7]           #gets year from file name        
            file_location = 'names\\'+ file_name  #presents where the files are located   
            
            #empty list to gather how many times a name occurs per year
            namePerYear = []        
            

            f = open(file_location)
            not_found = 0
            for line in f:
                name_data = line.strip('\n').split(',') 
                name_list = name_data[0]
                sex_list = name_data[1]
                occur = int(name_data[2])
                namePerYear.append(int(occur))
                
                if name_list == name and sex_list == sex:   #if name comes up we add it to our list
                    t_occur.append(int(occur))
                    not_found = 1

            if not_found == 0:
                t_occur.append(0) #this is so i can make sure it counts every year
            
          
            tNoccur = sum(namePerYear)
            instances = (t_occur[i])
            percent = (t_occur[i]/tNoccur)*1000 # per thousand instances
            data.append([year, instances ,percent]) #all the data
            i +=1
            f.close()

      
    
    #calls our function to calculate the average trendline
    avg = least_square(data)

    #creates a file to be itterated by matlab
    f1 = open('Name_data.txt', 'w')

    #orgnizes the file accordingly 
    f1.write('Name\tSex\tm1\tb1\tm2\tb2\n')
    f1.write('{}\t{}\t{}\t{}\t{}\t{}\n'.format(name,sex,avg[0],avg[1],avg[2],avg[3]))
    
    f1.write('Year\tOccurances\tPopularity\n')

    for i in range(len(data)):
        info = data[i]
        file_line = "{}\t{}\t{}\n".format(str(info[0]),str(info[1]), str(info[2]))

        f1.write(file_line)
    
    f1.close()

            
                
#function to calculate the average trendline        
def least_square(data):
    x1 = []
    x_squared = []
    y1 = []
    y2 = []
    xy = []
    xy2 = []

    for i in range(0,len(data)):
        years = int(data[i][0])
        instances = int(data[i][1])
        popularity = int(data[i][2])
        x1.append(years)
        x_squared.append(years**2)
        y1.append(instances)
        y2.append(popularity)
        xy.append(years*instances)
        xy2.append(years*popularity)

    E_xy = sum(xy)
    E_xy2 = sum(xy2)
    E_x1 = sum(x1)
    E_y1 = sum(y1)
    E_y2 = sum(y2)
    E_x_squared = sum(x_squared)
    n = len(x1)
    
    #instances
    numerator = E_xy - ((E_x1*E_y1)/n)
    denomenator = E_x_squared - ((E_x1**2)/n)

    m1 = numerator/denomenator
    b1 = (E_y1 - (m1*E_x1))/n

    m1 = round(m1,2)
    b1 = round(b1,2)

    #popularity
    numerator2 = E_xy2 - ((E_x1*E_y2)/n)
    denomenator2 = E_x_squared - ((E_x1**2)/n)
    
    m2 = numerator2/denomenator2
    b2 = (E_y2 - (m2*E_x1))/n

    b2 = round(b2)
    return m1,b1,m2,b2

