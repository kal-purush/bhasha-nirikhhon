import os
path=input("Enter full path of the folder !: ")
name=input("Enter your file name !: ")
mode=input("Enter the mode you in which you want to do !: ")

try:
    os.chdir(path)
    files = os.listdir(path)
    for file in files:
        filee=file.split(".")
        if(filee[0]==name):
            os.rename(f"{file}", f"{filee[0]}.{mode}")
            break
    else:
        print("The file name is not in the folder !: ")
        input("Press any key to exit !: ")
except:
    print("Something Went Wrong !: ")

