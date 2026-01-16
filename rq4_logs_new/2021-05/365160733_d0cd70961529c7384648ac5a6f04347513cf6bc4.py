import cloudmusic
import os

print("请输入想干的事情\n1.下载单曲音乐\n2.下载歌单\n3.下载专辑\n")
todo = input("你的选择是： ")
if (todo == "1"):
    songid = input("请输入歌曲id： ")
    music = cloudmusic.getMusic(songid)
    print("歌名：{}".format(music.name))
    print("歌手：{}".format(music.artist))
    print("专辑：{}".format(music.album))
    print("音频文件url：{}".format(music.url))
    os.system("aria2c --split 200 " + "\"" + music.url + "\"" + " " + "-o " +
              "\"" + music.name + "\"")
elif (todo == "2"):
    playlist1 = input("请输入歌单id： ")
    playlist = cloudmusic.getPlaylist(playlist1)
    for music in playlist:
        print("歌名：{}".format(music.name))
        print("歌手：{}".format(music.artist))
        print("专辑：{}".format(music.album))
        print("音频文件url：{}".format(music.url))
        os.system("aria2c --split 200 " + "\"" + music.url + "\"" + " " +
                  "-o " + "\"" + music.name + ".m4a" + "\"")
elif (todo == "3"):
    album1 = input("请输入专辑id： ")
    album = cloudmusic.getAlbum(album1)
    for music in album:
        print("歌名：{}".format(music.name))
        print("歌手：{}".format(music.artist))
        print("专辑：{}".format(music.album))
        print("音频文件url：{}".format(music.url))
        os.system("aria2c --split 200 " + "\"" + music.url + "\"" + " " +
                  "-o " + "\"" + music.name + ".m4a" + "\"")
else:
    print("瞎输什么呢")
    exit(1)