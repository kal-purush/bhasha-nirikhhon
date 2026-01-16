#coding=utf-8
"""
将数据集FDDB转换为tfrecords格式
"""
import tensorflow as tf
import numpy as np
import os
import matplotlib.pyplot as plt
import PIL.Image as Image

BATCH_SIZE=5
Annotations_dir="/home/yin/Documents/TFrecord/FDDB_data/FDDB-folds"
Image_name='/home/yin/Documents/TFrecord/FDDB_data/FDDB-folds/FDDB-fold-01.txt'
def get_file(file_dir,m0,m1):
    images=[]
    #labels=[]
    image_nums=[]
    labels=[]
    i=0
    filepath=file_dir+'/FDDB-fold-'+str(m0)+str(m1)+'-ellipseList.txt'
    imagepath=file_dir+'/FDDB-fold-'+str(m0)+str(m1)+'.txt'
    file=open(filepath)
    imagefile=open(imagepath)
    image_lines=imagefile.readlines()
    lines=file.readlines()
    #labels=[[]]*len(Image_name)
    datas=[[]]*len(image_lines)
    k=0
    image_num=[]
    while i<len(lines):
        image_name=[]
        label=lines[i][:-1]
        labels=np.append(labels,label.split('/')[-1])
        image_name='/home/yin/Documents/TFrecord/FDDB_data/originalPics/'+label+'.jpg'#file.readline()
        im_num=lines[(i+1)].strip()
        image_num=np.append(image_num,int(im_num))
        if int(im_num)==1:
            image_data=[]
            image_data=lines[(i+2)][:-1].split(' ')
            del image_data[5]
            for m in range(len(image_data)):
                image_data[m]=float(image_data[m])
            datas[k]=np.append(datas[k],image_data)
        else:
            datas[k]=[[]]*int(im_num)
            j=0
            for j in range(int(im_num)):
                x=[]
                x=lines[(i+2+j)][:-1].split(' ')
                del x[5]
                for m in range(len(x)):
                    x[m]=float(x[m])
                datas[k][j]=np.append(datas[k][j],x)
        k=k+1
        i=i+2+int(im_num)
        images=np.append(images,image_name)
    return images,datas,labels

def int64_feature(value):
    if not isinstance(value,list):
        value=[value]
    return tf.train.Feature(int64_list=tf.train.Int64List(value=value))
def bytes_feature(value):
    return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

def convert_to_tfrecords(images,labels,save_path,name):
    filename=os.path.join(save_path,name+".tfrecords")
    writer=tf.python_io.TFRecordWriter(filename)
    for i in range(np.shape(images)[0]):
        image_open=Image.open(images[i])
        image2=image_open.resize([400,400])
        image_raw=image2.tobytes()
        label=labels[i]
        example=tf.train.Example(features=tf.train.Features(feature={
                    'image_raw':bytes_feature(image_raw),
                    'label':bytes_feature(label)
        }))
        writer.write(example.SerializeToString())
    writer.close()
def read_from_tfrecords(tf_file,batch_size):
    filequeue=tf.train.string_input_producer([tf_file])
    reader=tf.TFRecordReader()
    _,serialize_example=reader.read(filequeue)
    img_feature=tf.parse_single_example(serialize_example,features={
                'image_raw':tf.FixedLenFeature([],tf.string),
                'label':tf.FixedLenFeature([],tf.string)
    })
    image_raw=tf.decode_raw(img_feature['image_raw'],tf.uint8)
    image3=tf.reshape(image_raw,[400,400,3])
    label=tf.cast(img_feature['label'],tf.string)

    #return image_batch,np.reshape(label_batch,[batch_size])
    #return image_batch,label_batch
    return image3,label

def plot_images(images):
    plt.title('off')
    #images=Image.open(StringIO(images))
    plt.imshow(images)
    plt.show()

Save_path="/home/yin/Documents/TFrecord/FDDB_data"
tf_path="/home/yin/Documents/TFrecord/FDDB_data/DATA3.tfrecords"
image,data,label=get_file(Annotations_dir,1,0)
#image=['/home/yin/Documents/TFrecord/FDDB_data/img_18.jpg']
#image=['/home/yin/Documents/TFrecord/FDDB_data/img_15.jpg']
#label='img_15'
if not os.path.exists(tf_path):
    convert_to_tfrecords(image,label,Save_path,"DATA3")
image_b,label_b=read_from_tfrecords(tf_path,BATCH_SIZE)
image_batch,label_batch=tf.train.shuffle_batch([image_b,label_b],
                   batch_size=BATCH_SIZE,
                   num_threads=16,
                   capacity=10,
                   min_after_dequeue=5)
image_last=[]
label_last=[]
init=tf.initialize_all_variables()
with tf.Session() as sess:
    sess.run(init)
    coord=tf.train.Coordinator()
    threads=tf.train.start_queue_runners(coord=coord)
    i=0
    try:
        while not coord.should_stop() and i<1:
            image,label=sess.run([image_batch,label_batch])

            #plot_images(image_batch)
            #print(label_batch)
            i=i+1
            print(image.shape,label.shape)
            for k in range(BATCH_SIZE):
                #plt.subplot(1,2,k+1)
                #image3=Image.fromarray(image2.astype(np.uint8))
                img=Image.fromarray(image[k].astype(np.uint8))
                img.save(Save_path+'/'+str(k)+'.jpg')
    except tf.errors.OutOfRangeError:
        print("done!")
    finally:
        coord.request_stop()
    coord.join(threads)