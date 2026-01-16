
names = ["Peter","Jakob","Antoine"]
#names = ["Peter","Jakob"]
#names = ["Peter","Jakob","Annette","JOhn",'Aylien','anasis']

def likes(names):
    if (len(names) == 0):
        print('no one likes this')
    elif (len(names) == 1):
        print(names[0]+' likes this')
    elif (len(names) == 2):
        output = "{} and {} like this"
        print(output.format(names[0],names[1]))
    elif (len(names) == 3):
        output = "{}, {} and {} like this"
        print(output.format(names[0],names[1],names[2]))
    elif (len(names) >= 4):
        output = "{} , {} and {} others like this"
        print(output.format(names[0],names[1],len(names)-2))
    pass

likes(names)