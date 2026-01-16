import requests
import datetime
import os
import winsound
import bs4 as bs
import webbrowser
from PIL import Image
from io import BytesIO
from urllib.request import Request, urlopen
from multiprocessing.dummy import Pool as ThreadPool

def link_parser(image):
    image = image.replace('thumbnails', '')
    link = image.replace('/th', '/i').replace('small', 'big').replace('//t', '//i').replace('/t/', '/i/').replace('_t', '')
    if 'imgcandy' in link:
        link = link.replace('imgc', 'i.imgc').replace('/upload', '')
    if 'pixhost' in link:
        link = link.replace('//i', '//img').replace('iumbs', 'images')
    if 'img.yt' in link:
        link = link.replace('img.yt', 's.img.yt').replace('/upload', '')

    return link

def link_opener(url, jump):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    soup = bs.BeautifulSoup(webpage, 'lxml')
    images = []
    
    for img in soup.findAll('img'):
        image = str(img.get('src'))
        if image[:4]=='http' and '' in image:
            images.append(link_parser(image)) 
    i=0
    k=3
    for img in images:
        i+=1
        url = img
        if i == k:
            k +=int(len(images)/jump)
            webbrowser.open_new(url)
    
        print(str(i)+'. '+img)

def multiple_threads(urls, names):
    start_all = datetime.datetime.now()

    for url, name in zip(urls, names):
        url_link = url
        req = Request(url_link, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read()
        soup = bs.BeautifulSoup(webpage, 'lxml')
        images = []
    
        #List with urls
        for img in soup.findAll('img'):
            image = str(img.get('src'))
            if image[:4]=='http' and '' in image:
                images.append(link_parser(image)) 

        #numbers for naming files   
        limit = len(images)
        i = [x for x in range(1, limit+1)]
    
        folder_name = f'{name}'
        folder_path = 'F:\Pyk\Photos\\'+folder_name+'\\'
        print(folder_name)

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f'Created folder: {folder_path}')
            
        stuff = os.listdir(folder_path)
        stuff = [x for x in stuff if 't_' not in x]
        stuff = [x.replace('.jpg', '') for x in stuff]
        buuu = []
    
        def everything(images, i):
            now = str(datetime.datetime.now())
            out_of = limit
            
            if str(i).zfill(3) not in stuff:  
                if i%10 == 0:
                    print('Processing... '+str(i)+'/'+str(out_of)+'\t'+'\t'+now)
                new = 'F:\Pyk\Photos\\'+folder_name+'\\'+''+str(i).zfill(3)+'.jpg'
    
                try:
                    img = Image.open(BytesIO(requests.get(images).content))
                    img.save(new)
                    
                except:
                    buuu.append(i)
                    pass
                
        start = datetime.datetime.now()
    
        pool = ThreadPool(16) 
        pool.starmap(everything, zip(images, i))
        pool.close() 
        pool.join() 
        
        print('Following {} went to shit:'.format(len(buuu)))
        print(set(buuu))
    
        end = datetime.datetime.now()
        winsound.PlaySound('SystemExit', winsound.SND_ALIAS)
        print('Done in '+str(end-start))
        print()
    
    end_all = datetime.datetime.now()
    print('Done \'em all in '+str(end_all-start_all))

def from_post(page, url, postcount, postnames):
    proper_start = datetime.datetime.now()

    req = Request(url+page, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    soup = bs.BeautifulSoup(webpage, 'lxml')
    posts = soup.find_all('li')
    
    k = 0
    lel = len(postcount)
    
    for postname, req_post in zip(postnames, postcount):
        k += 1
        images = []
        for post in posts:
            names = post.findAll('a')
            for name in names:
                if '#'+str(req_post) in name:          
                    interesting = post.find_all('img')
                    for img in interesting:
                        check = str(img.get('src'))
                        if check[:4]=='http':
                            images.append(link_parser(check)) 

        #numbers for naming files   
        limit = len(images)
        i = [x for x in range(1, limit+1)]
    
        folder_name = postname
        folder_path = 'F:\Pyk\Photos\\'+folder_name+'\\'
    
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        def everything(images, i):
            now = str(datetime.datetime.now())
            out_of = limit
            if i%20 == 0:
                print('Processing... '+str(i)+'/'+str(out_of)+'\t'+'\t'+now)
            img = Image.open(BytesIO(requests.get(images).content))
            new = 'F:\Pyk\Photos\\'+folder_name+'\\'+''+str(i).zfill(3)+'.jpg'
            img.save(new)
    
        start = datetime.datetime.now()
    
        pool = ThreadPool(16) 
        pool.starmap(everything, zip(images, i))
        pool.close() 
        pool.join() 
    
        end = datetime.datetime.now()
    
        print(f'{k}/{lel} Done in {end-start}')

    proper_end = datetime.datetime.now()
    winsound.PlaySound('SystemExit', winsound.SND_ALIAS)
    print('Done \'em all in '+str(proper_end-proper_start))


postcount = [61,62,66,67,69,72,73]
postnames = ['Anita C - Lodels', 'Anita C - Bringing', 'Anita C - Ivimas', 
             'Anita C - Totally', 'Anita C - To The Top', 'Anita C - Sensix', 
             'Anita C - Velian']
page = '5'
url = 'https://vipergirls.to/threads/1586427-Anita-Anita-C-Anita-Silver-Arina-Danica-Danita-Luisa-Mocca-Vasilisa-Mudraja/page'

#from_post(page, url, postcount, postnames)

urls = ['https://vipergirls.to/threads/1280907-Engelie-Extreme-Perspective-(X45)-10000px?highlight=Engelie',
       'https://vipergirls.to/threads/639846-Engelie-Tropical-Garden-x59?highlight=Engelie']
names = ['Engelie - Extreme Perspective', 'Engelie - Tropical Garden']

#multiple_threads(urls, names)


url = 'https://vipergirls.to/threads/639846-Engelie-Tropical-Garden-x59?highlight=Engelie'
#link_opener(url, 3)