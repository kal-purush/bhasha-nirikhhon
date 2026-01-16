from pytube import YouTube
import urllib
import lxml.html
import pafy

print("***YouTube Playlist Downloader***\n")
text_file = open("filePath.txt", "r+")
text_line = text_file.read()
if text_line == "":
    path = raw_input("Enter path for storing downloaded videos: ")
    text_file.write(path)
else:
    path = text_line
text_file.close()
line = raw_input("Enter complete url of playlist to be downloaded: ")
playlist = line.split('=')[1]
num = 1
chk = 1
flag = 'check'

def download():
    videoID = link.split('=')[1]
    url = "http://www.youtube.com/watch?v=" + videoID   
    yt = YouTube(url)
    video = pafy.new(url)
    name = video.title
    
    yt.set_filename(name)
    video = yt.get('mp4', '360p')
    
    print("\nDownloading...(Might take several minutes depending on size of video)")
    video.download(path)
    print("Download completed")

connection = urllib.urlopen('http://www.youtube.com/playlist?list=' + playlist)
dom = lxml.html.fromstring(connection.read())

for link in dom.xpath('//a/@href'):
    if 'watch?v=' in link and flag != link:
        if chk > 2:
            download()
            num += 1
            flag = link
        else:
            chk += 1
    else:
        flag = 'check'