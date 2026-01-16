import os
import urllib.request as ur
import sys
import bs4 as bs
def main():
    imtype=input("Enter the Name of the folder to save image ")
    url=input("Enter the url you want to scrap images from : ")
    headers={}
    headers['User-Agent'] = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"
    req=ur.Request(url,headers=headers)
    res=ur.urlopen(req).read()
    soup=bs.BeautifulSoup(res,"lxml")
    user=os.getlogin()
    os.chdir("C://Users//"+user+"//Pictures")
    try:
        os.mkdir(imtype)
    except:
        pass
    os.chdir(imtype)
    i=0
    for img in soup.find_all('img'):
        imglink=img.get('src')
        if(imglink=='None'):
            imglink=img.get('data-src')
        if(str(imglink)!='None'):
            try:
                name='cow'+str(i)+'.jpg'
                f=open(name,'wb')
                f.write(ur.urlopen(str(imglink)).read())
                f.close()
            except:
                pass
            print(imglink)
            i+=1
if __name__=="__main__" :
    main()