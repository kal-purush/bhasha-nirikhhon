import httplib2
import pdfcrowd
from bs4 import BeautifulSoup, SoupStrainer
import pdfkit
import sys
 
http = httplib2.Http()
t= 'https://www.geeksforgeeks.org/tag/amazon/page/'
p='1'
s=t+p
to_crawl=[]
crawled=[]
i=0
#to_crawl.append(s)
status, response = http.request(s)
#crawled.append(s)
soup = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('a'))
last = soup.find(class_="last")
if last.has_attr('href'):
    li=last['href']
lastfour = li[-4:]
lastthree = lastfour[0:3]
print(lastthree)
a = int(lastthree)

for j in range(50):
    s=t+str(j+1)
    print(s)
    status, response = http.request(s)
    mycount = 0
    for link in BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('a')):
            #print(link)
            if link.has_attr('href'):
                li=link['href']
                mycount+=1
                to_crawl.append(li)

               
    # print(mycount)
    # print(len(to_crawl))
    # #print(to_crawl)
             
    amazon=[ ]
    print(len(amazon))
     
    for st in to_crawl:
        #print(st)
        if st.find('amazon')>=0 and st.find('#')<0 and st.find('tag')<0 and st.find('forum')<0:
            #print("string is " + st)
            amazon.append(st)
     
     
     
    print("Finished")
    print(len(amazon))
    #print(amazon)
            
    # for page in amazon:
    #     print(page)

    # for page in amazon:
    #     save_as_pdf(page)


k=0

listOfLink = []
for k in range(len(amazon)):
    if k%2==0:
        listOfLink.append(amazon[k])

print(len(listOfLink))



count=0
pdfs = [ ]

def save_as_pdf(s):
    global i
    output_file = open('amazon'+str(i)+'.pdf', 'wb')
    print(output_file.name)
    pdfs.append(output_file.name)
    i=i+1
    status, response = http.request(s)
    soup = BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('article'))
    print(s)
    original_stdout = sys.stdout # Save a reference to the original standard output

    with open('temp'+str(i)+'.html', 'w') as f:
        sys.stdout = f # Change the standard output to the file we created.
        print('<html><body>')
        print('<style>body {background-color: #fcf;color:#black; font-size:20px;font-family:Helvetica;}</style>')
        #print(soup)
        print(soup.encode("utf-8"))
        print('</html></body>')
        sys.stdout = original_stdout

    pdfkit.from_file('temp'+str(i)+'.html', output_file.name)
    output_file.close()

for item in listOfLink:
    save_as_pdf(item)
    print(item)


from PyPDF2 import PdfFileMerger, PdfFileReader
merger = PdfFileMerger()
for file_name in pdfs:
    with open(file_name, 'rb') as f:
        merger.append(f, import_bookmarks=False )

merger.write('result'+str(i)+'.pdf')