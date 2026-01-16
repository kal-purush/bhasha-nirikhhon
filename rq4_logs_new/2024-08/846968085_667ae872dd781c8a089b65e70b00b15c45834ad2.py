import os
import  shutil


def moveFile(dir:str) -> None:
    imgfolder = os.listdir(dir)

    for folder in imgfolder:
        dir = os.path.join(dir, folder)
        for lastdir in os.listdir(dir):
            createdimgfolder = os.path.join(dir, lastdir, "imgs/")
            if not os.path.exists(createdimgfolder):
                os.mkdir(os.path.join(dir, lastdir, "imgs/"))
            
            for img in os.listdir(os.path.join(dir, lastdir)):
                if "ZZH" in img:
                    src = os.path.join(dir, lastdir, img)
                    dst = os.path.join(dir, lastdir, "imgs/")
                    shutil.copy(src, dst)
    
    print("Done")
    
if __name__ == "__main__":
    folders = ["test", "train", "valid"]
    for folder in folders:  
        dir = f"C:/Users/vivia/OneDrive/桌面/lab/split_w_diease/split_w_diease/{folder}"
        moveFile(dir)