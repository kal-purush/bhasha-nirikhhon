from string import ascii_uppercase
from string import ascii_lowercase
pdffiletouse = 'Lecture_Schedule2.pdf'

datesfile = open("finalwrite.txt","w") 
months = []
current_year = 2021



for i in range(1,31):
    months.append(str(i))

monthsname = ["J","F","M","A","S","O","N","D"]
monthscontinue = ["a", "n", "u", "r", "y", "e", "m", "b","r","c","h","p","i","l","y","g","s","t","o","v"]

al = list(ascii_uppercase)

with open('output.txt', 'r') as f:
    all_lines = []
    new_line = []
    c = 1
    for line in f.readlines():
        for chars in line:
            if(c == 1 and (chars in monthsname)):
                c=c+1
                new_line.append(chars)
           
            if(c > 1 and new_line[1:] not in al and (new_line[0] in monthsname or (chars in monthscontinue or chars in months or chars == " " or chars == "," or chars =="\n"))):
                c=c+1
                if(not(chars in monthsname)):
                    new_line.append(chars)
            else:
                new_line = []
                
                
            
            if(c==15):
                k=0
                
                if(len(new_line)>4):
                    
                    for i in new_line:
                        if(i in ascii_uppercase):
                            k=k+1
                    if(k<2):
                        
                        l= ''.join(new_line)
                        q = ""
                        for i in l:
                            if i != '\n':
                                q = q + i
                        s = str(q)
                        g=0
                        flag = 0
                        for i in s:
                            if (flag == 0 and (i >='a' and i<='z')):
                                g=g+1
                                flag = 1

                            if ((i >='1' and i <='9')):
                                g=g+1

                        if(g>=2):
                            q = str(current_year)+' ' + q
                            datesfile.write(q)
                            datesfile.write("\n")
                            print(new_line)
                        
                #formatting date
                #formatted_date = datetime.date.strftime(current_day, "%m/%d/%Y")
                new_line = []
                c = 1
datesfile.close()
# with open('finalwrite.txt', 'r') as f:
#     txt = f.read().replace(' ', '')

# with open('finalwrite.txt', 'w') as f:
#     f.write(txt)


import datetime
from dateutil.parser import parse
import sys
sys.stdout = open('finaldateouput.txt','wt')
with open('finalwrite.txt', 'r') as f:
    for line in f.readlines():
        from dateutil.parser import parse
        formatted_date = datetime.date.strftime(parse(line), "%m/%d/%Y") 
        today=datetime.datetime.strftime(parse(formatted_date),"%m/%d/%Y")
        today=today[:-4]+today[-2:]
        print(today)




         