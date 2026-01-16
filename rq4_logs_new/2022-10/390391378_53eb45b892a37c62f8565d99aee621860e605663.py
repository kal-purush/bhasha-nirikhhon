#!python
import sys
from pytube import YouTube
from os import system, remove

#function to convert illegal characters to a valid filename
def nameConvert(name):
    name = name.encode("cp850","replace").decode("cp850")
    #list of command line characters that are not allowed
    specialCharacters = ['\\','/',':','*','?','"',"'",';','!','@','#','$','%','^','&','*','(',')','_','+','=','{','}','[',']','|','~','`','.','-','<','>']
    for i in name:
        if i in specialCharacters:
            name = name.replace(i,"_")

    name = name.replace(" ", "_")
    name = name.replace("___", "_")
    name = name.replace("____", "_")
    name = name.replace("_____", "_")
    name = name.replace("______", "_")
    name = name.replace("__", "_")
    length = len(name)
    ending = ['.','_','-']
    for i in ending:
        if name.endswith(i):

            name = name[:length-1]

    return name



# a function to download only audio
def downloaderaudio(url):
    yt = YouTube(url)
    title = yt.title
    print(title)
    title = nameConvert(title)
    print("Downloading...")
    audio = yt.streams.filter(only_audio=True)
    audio.first().download("./", filename= title + ".mp3")
    print("Done")

def downloader(url):
    yt = YouTube(url)
    #Title of video
    title = yt.title
    
    print("Title: " + title)
    video = yt.streams.filter(only_video=True)
    audio = yt.streams.filter(only_audio=True)
    print("Downloading...")
    # download video and audio and then merge them together
    video.first().download("./",filename="temp.mp4")
    audio.first().download("./",filename="temp.mp3")
    print("Downloaded")
    # merge video and audio
    print("Merging...")
    args = '''ffmpeg -loglevel quiet -stats -i "temp.mp4" -i "temp.mp3" -c:v copy -c:a aac -strict experimental "temp2.mp4"'''
    system(args)
    print("Merged")
    # delete temp files
    print("Deleting temp files...")
    remove("temp.mp4")
    remove("temp.mp3")
    title = nameConvert(title)
    args = 'rename "temp2.mp4" "' + title + '.mp4"'
    system(args)
    print("Done")
    return

def downloaderlow(url):
    yt = YouTube(url)
    title = yt.title
    print(title)
    ys = yt.streams.get_highest_resolution()
    print("Low Quality")
    print("Downloading...")
    # Download video
    ys.download()
    print("Downloaded")


def main():
    url = ''
    if len(sys.argv) > 1:
        if len(sys.argv) == 2:
            url = sys.argv[1]
            downloader(url)
        elif len(sys.argv) == 3:
            if sys.argv[1] == 'l':
                print("Low Quality")
                url = sys.argv[2]
                downloaderlow(url)
            elif sys.argv[1] == 'a':
                print("Audio")
                url = sys.argv[2]
                downloaderaudio(url)
            elif sys.argv[1] == 'h':
                print("High Quality")
                url = sys.argv[2]
                downloader(url)
            else:
                print("Invalid Arguments")
                print("Usage: python ytd.py [l] [url]")
    else:
        print("Invalid Arguments")
        print("Usage: python ytd.py [l] [url]")
        url = input('Enter url: ')
        quality = input('Enter quality "l" for low and "h" for high or a for audio only: ')
        if quality == "" or quality == "h":
            downloader(url)
        elif quality == "l" and url != "":
            print("Low Quality")
            downloaderlow(url)
        elif quality == "l" and url == "":
            print("Invalid Arguments")
            print("Usage: python ytd.py [l] [url]")
        elif quality == "a" and url != "":
            print("Audio")
            downloaderaudio(url)
        else:
            print("Invalid Arguments")
            print("Usage: python ytd.py [l] [url]")
        return

main()