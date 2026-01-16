importglob

name_array=[]
name_array_2=[]
path_video=""
path_img_txt="./results.txt"
withopen(path_img_txt)asf:
lines=f.readlines()
foriinlines:
path_img_txt+=1
name_array.append(i)

forvideo_nameinglob.glob(path_video+"/*"):
len_check=len(glob.glob(path_video+"/*"))
ifvideo_name.split("/")[-1]notinname_array:
name_array_2.append(video_name)

path_img_txt=open("./results.txt","w")
fornameinname_array:
path_img_txt.write(name)
forname_2inname_array_2:
path_img_txt.write(name_2+",0\n")

print(f"Check:{len_check}vs{len(name_array)+len(name_array_2)}")