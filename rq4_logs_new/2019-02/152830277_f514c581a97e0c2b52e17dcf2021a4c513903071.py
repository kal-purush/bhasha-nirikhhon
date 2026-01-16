import os
import cv2
import glob
import random
import os
os.environ["CUDA_VISIBLE_DEVICES"] = "1"

def getImagesTxt():
    # fd = open('/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align/img_path_spoof_pop_stars.txt','w')
    # path = '/home/yaoqiang/data/spoof_pop_stars'
    #
    # fd = open('/home/yaoqiang/data/data_collection/live_detect/pic_fail/img_path2.txt', 'w')
    # path = '/home/yaoqiang/data/data_collection/live_detect/pic_fail/pics'


    # fd = open('/home/yaoqiang/data/beijing_huaxin/img_path_live_bjhx.txt','w')
    # path = '/home/yaoqiang/data/beijing_huaxin/live'

    fd = open('/home/yaoqiang/data/warp/train_live_frame/bjhx_warp_frame.txt', 'w')
    path = '/home/yaoqiang/data/warp/train_live_frame/bjhx_warp_frame'

    # dstpath = '/home/yaoqiang/PycharmProjects/liveness/images/'
    for dirpath,dirnames,filenames in os.walk(path):
        for filename in filenames:
            if ( False == ('.jpg' in filename)):
                continue
            pic = os.path.join(dirpath,filename)
            # img = cv2.imread(pic)
            # img = cv2.resize(img,(224,224))
            # cv2.imshow('test',img)d
            # cv2.waitKey(0)
            # dstpic = os.path.join(dstpath,filename)
            # cv2.imwrite(dstpic, img)
            # if 'cat' in filename:
            #     fd.write(filename+' 1\n')
            # if 'dog' in filename:
            #     fd.write(filename+' 0\n')
            fd.write(pic + '\n')
            # fd.write(filename + '\n')
        # cv2.destroyAllWindows()

    fd.close()
    return

def getImagesTxtShuffle():
    # root_path = '/home/yaoqiang/data/data_collection/face_tyre4.0/*/'
    root_path = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align/*/'
    # root_path = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align/live_huaxin/'
    image_paths = glob.glob(root_path + "*.*")

    random.shuffle(image_paths)
    path_len = len(image_paths)
    print(path_len)

    # fd = open('/home/yaoqiang/data/data_collection/face_tyre4.0/face_tyre_path.txt', 'w')
    # fd = open('/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align/train_all_path.txt', 'w')
    fd = open('/home/yaoqiang/PycharmProjects/liveness/lmdb/train_lmdb.txt', 'w')

    for i in (range(path_len)):
        imagepath = image_paths[i]
        if 'spoof' in imagepath:
            fd.write(imagepath + '\t ' + '1\n', )
        else:
            fd.write(imagepath + '\t' + '0\n', )
    fd.close()

def getImagesTxt2(test_set, dstPath):
    f = open(test_set, 'r')
    i = 0
    lines = f.readlines()
    for aline in lines:
        i = i + 1
        aline = aline.strip()
        img_path = aline.strip().split('#')[0]
        img_color = cv2.imread(img_path)
        [roix, roiy, x2, y2] = aline.split('#')[1].split(' ')
        roiw = int(x2) - int(roix)
        roih = int(y2) - int(roiy)
        roix = int(roix)
        roiy = int(roiy)
        crop_img = img_color[roiy:roiy + roih, roix:roix + roiw]
        scale_img_tmp = cv2.resize(crop_img, (128, 128))
        img2 = cv2.cvtColor(scale_img_tmp, cv2.COLOR_BGR2HSV)
        # cv2.imshow('img',scale_img_tmp)
        # cv2.waitKey(0)
        imgPath = dstPath + str(i) + '.jpg'
        cv2.imwrite(imgPath, img2)
    return


# fd = open('/home/yaoqiang/data/nz432.txt','r')
# for line in fd:
#     print(line)

# with open('/home/yaoqiang/data/nz432.txt','r') as fd:
#     while True:
#         line = fd.readline()
#         if not line:
#             break
#         print(line)

def getImagesTxtForCaffe():
    # root_path = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train/*/'
    root_path = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align/*/'
    # root_path = '/home/yaoqiang/data/data_collection/face_tyre4.0/*/'
    # image_paths = glob.glob(root_path + "*.jpg")
    image_paths = glob.glob(root_path + "*.*")

    random.shuffle(image_paths)
    path_len = len(image_paths)
    print(path_len)

    # dstpath = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_caffe_v1.0.5'
    # fd = open('/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/img_for_caffe_v1.0.5.txt', 'w')

    # dstpath = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align_caffe_v1.0.0'
    # fd = open('/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/align_img_for_caffe_v1.0.0.txt', 'w')

    # dstpath = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align_luv_caffe_v1.0.7'
    # fd = open('/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/align_img_luv_for_caffe_v1.0.7.txt', 'w')

    # dstpath = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align_yCrCb_caffe_v1.0.0'
    # fd = open('/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/align_img_yCrCb_for_caffe_v1.0.0.txt', 'w')

    # dstpath = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align_hsv_caffe_v1.0.0'
    # fd = open('/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/align_img_hsv_for_caffe_v1.0.0.txt', 'w')

    dstpath = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align_caffe_v1.0.7_ex2'
    fd = open('/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/align_img_for_caffe_v1.0.7_ex2.txt', 'w')

    for i in (range(path_len)):
        imagepath = image_paths[i]

        img_color = cv2.imread(imagepath)
        if img_color is None:
            print(imagepath)
            continue


        img_color = cv2.resize(img_color, (128,128))

        # img_luv = cv2.cvtColor(img_color, cv2.COLOR_BGR2LUV)
        # img_ycrcb = cv2.cvtColor(img_color, cv2.COLOR_BGR2YCR_CB)
        # img_hsv = cv2.cvtColor(img_color, cv2.COLOR_BGR2HSV_FULL)
        # img_lab = cv2.cvtColor(img_color, cv2.COLOR_BGR2LAB)

        imgName = str(i) + '.jpg'
        dstpic = os.path.join(dstpath,imgName)
        # cv2.imwrite(dstpic, img_color)
        cv2.imwrite(dstpic, img_color)
        # cv2.imwrite(dstpic, img_ycrcb)
        # cv2.imwrite(dstpic, img_hsv)
        # cv2.imwrite(dstpic, img_color)
        if 'spoof' in imagepath:
            fd.write(imgName + ' ' + '1\n', )
        else:
            fd.write(imgName + ' ' + '0\n', )

        # if 'tyres' in imagepath:
        #     fd.write(imagepath + ' ' + '1\n', )
        # else:
        #     fd.write(imagepath + ' ' + '0\n', )
    fd.close()

live = '/darray_det/liveness/SiW_images/Test/test_live_boxes.txt'
spoof = '/darray_det/liveness/SiW_images/Test/test_spoof_boxes.txt'
dstPath ='/home/yaoqiang/data/SIW/test_hsv/spoof/'



def getImagesTxt3():
    root_path = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/test_align/spoof_lab/*/'
    image_paths = glob.glob(root_path + "*.*")
    dstPath = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align/spoof_lab/'

    random.shuffle(image_paths)
    path_len = len(image_paths)
    print(path_len)

    for i in (range(path_len)):
        imagepath = image_paths[i]
        img_color = cv2.imread(imagepath)

        imgName = str(i) + '.jpg'
        dstpic = os.path.join(dstPath,imgName)
        cv2.imwrite(dstpic, img_color)
    return

def getImagesTxt5():
    root_path = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/test_align/spoof_lab2/*/'
    image_paths = glob.glob(root_path + "*.*")
    dstPath = '/home/yaoqiang/data/data_collection/live_detect/SIW/224x224/train_align/spoof_lab/'

    baseNum = len(glob.glob(dstPath + "*.*"))
    random.shuffle(image_paths)
    path_len = len(image_paths)
    print(path_len)

    for i in (range(path_len)):
        imagepath = image_paths[i]
        img_color = cv2.imread(imagepath)
        j = baseNum + i + 1
        imgName = str(j) + '.jpg'
        dstpic = os.path.join(dstPath,imgName)
        cv2.imwrite(dstpic, img_color)
    return


if __name__ == "__main__":
    # getImagesTxt2(spoof, dstPath)
    # getImagesTxt()
    # getImagesTxt3()
    # getImagesTxt5()
    getImagesTxtForCaffe()
    # getImagesTxtShuffle()

      