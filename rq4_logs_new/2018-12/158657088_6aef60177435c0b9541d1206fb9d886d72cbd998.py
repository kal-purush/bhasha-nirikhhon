#向文本文件中写入路径

#root 所指的是当前正在遍历的这个文件夹的本身的地址
#dirs 是一个 list ，内容是该文件夹中所有的目录的名字(不包括子目录)
#files 同样是 list , 内容是该文件夹中所有的文件(不包括子目录)
import os

from chardet import detect
def get_file_info(file_path):
	with open(file_path,"rb") as fp:
	#
		#检测文件编码，编码信息保存到code中
		encode = detect(fp.read())['encoding']
		info.write("Encoding:"+str(encode)+"\n")

	#引用第三方库：chardet
	# pip install chardet 命令指示符自动安装chardet包
	line_count,blank_count = 0,0
	with open(file_path,'r',encoding=encode) as fp:#'r'是以源文件的方式读取
		while True:
			line = fp.readline()
			if not line:
				break
			line_count += 1
			if len(line.strip()) == 0:
				blank_count += 1
	info.write(str(line_count)+"Lines)"+str(blank_count)+"blanks)"+"\n")

root_path = os.getcwd()
#检测有多少文件夹
#a = os.path.isfile(r"C:\Users\Administrator\Desktop\root\a.txt")#测试某文件是否在文件夹下
#print(a) 测试某文件是否在某文件夹下，输出的是True或False
dir_count,file_count = 0,0
info = open("file_info.txt",'w')
for root,dirs,files in os.walk(root_path):
	if not os.path.isfile(root):
		file_count += 1
	for f in files:
		file_path = os.path.join(root,f)
		if os.path.isfile(os.path.join(file_path)):
			dir_count += 1
		info.write("Filename:"+os.path.splitext(f)[0]+"\n")
		info.write("Type:"+os.path.splitext(f)[1]+"\n")
		info.write("Size:"+str(os.path.getsize(file_path))+"Bytes"+"\n")#统计文件大小
		info.write("Location:"+root+"\n")
		get_file_info(file_path)
		info.write("-"*60+"\n")
#print(str(dir_count-1)+"Folders"+str(file_count)+"files")
		#print(os.path.join(root,f))

		#print(f)#所有文件夹下的文件
info.write(str(dir_count-1)+"Folders"+"\n")
info.write(str(file_count)+"Files"+"\n")

#以下是关于路径的代码
root_path = os.getcwd()  #获取当前根文件的路径
offset = len(root_path.split("\\"))
for root,dirs,files in os.walk(root_path):  # 遍历文件夹
	current_path = root.split("\\")
	#path_list = current_dir.split("\\")
	level = len(current_path) - offset
	info.write("\t"*level+"\\"+current_path[-1]+"\n")
	for f in files:
		#file_name = os.path.splitext(f)[0]  #文件名 文件扩展名 分离
		info.write("\t"*(level+1)+f+"\n")