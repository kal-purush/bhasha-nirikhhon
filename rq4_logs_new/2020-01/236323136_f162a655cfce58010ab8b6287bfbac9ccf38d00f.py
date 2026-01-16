import cv2
import numpy as np
import tensorflow as tf
import statistics
from pathlib import Path
import csv

global img_list
img_list = []

global img_holes_list
img_holes_list = []

global hole_indexes
hole_indexes = []

global y_true
y_true = []

global size_hole
size_hole = 10


# this function saves the loss to csv file
def write_losses_to_csv(loss_list, loss_avg, s):
    loss_list.append(loss_avg)
    with open("loss_CNN_{}.csv".format(s), "a") as fp:
        wr = csv.writer(fp, dialect='excel')
        wr.writerow(loss_list)


# this function, makes rectangle hole of sizeXsize starting from random i and random j
# and save the hole_image in img_holes_list
def make_hole_list():
    img_holes_list.clear()
    hole_indexes.clear()
    for img in img_list:
        start_i = np.random.random_integers(31, 215)
        start_j = np.random.random_integers(31, 215)
        hole_indexes.append((start_i, start_j))
        hole_img = img.copy()
        hole_img[start_j:start_j + size_hole, start_i: start_i + size_hole] = 0
        img_holes_list.append(hole_img)


# this function receives path to directory and adds them to img_list
def create_image_array(dir_path):
    img_list.clear()
    path_list = Path(dir_path).glob("*")
    for path in path_list:
        img_path = str(path)
        img = cv2.imread(img_path)
        img_list.append(img)


# this function returns the true value of the current batch pixels
def find_y_true():
    y_true.clear()
    counter = 0
    for img in img_list:
        start_i, start_j = hole_indexes[counter]
        roi_img = img[start_j: start_j + size_hole, start_i: start_i + size_hole]
        counter += 1
        y_true.append(roi_img)


# this function receives i,j and returns features list,y_true
def find_data_x_y():
    data_x = img_holes_list
    find_y_true()
    data_y = np.array(y_true)
    return data_x, data_y


def next_batch(counter_batch, path, num_dir):
    images_path = path + "/{}".format(counter_batch)
    # print("Start to read the images from batch number: ", counter_batch)
    counter_batch += 1

    if counter_batch == num_dir:
        print("Starts new epoc")
        counter_batch = 0

    create_image_array(images_path)
    np.random.shuffle(img_list)
    # define the size of hole in the images
    make_hole_list()

    return counter_batch


def train():
    # variables
    alpha = 0.001
    batch_size = 50
    num_of_batches = 6569
    graph_counter = 0
    loss_batch_list = []
    loss_100_batch_avg_list = []
    counter_batch = 0
    path = "/home/shaynaor/Downloads/image_inpainting_dataset/train_images_batches"


    x = tf.placeholder(tf.float32, shape=[None, 256, 256, 3], name="x")
    y_ = tf.placeholder(tf.float32, shape=[None, 10, 10, 3], name="y_")


    W_conv1 = tf.Variable(tf.truncated_normal([5, 5, 3, 8], stddev=0.1), name="W_conv1")
    b_conv1 = tf.Variable(tf.constant(0.1, shape=[8]), name="b_conv1")

    h_conv1 = tf.nn.relu(tf.nn.conv2d(x, W_conv1, strides=[1, 1, 1, 1], padding='SAME') + b_conv1, name="h_conv1")
    h_pool1 = tf.nn.max_pool(h_conv1, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME', name="h_pool1")


    W_conv2 = tf.Variable(tf.truncated_normal([5, 5, 8, 16], stddev=0.1), name="W_conv2")
    b_conv2 = tf.Variable(tf.constant(0.1, shape=[16]), name="b_conv2")

    h_conv2 = tf.nn.relu(tf.nn.conv2d(h_pool1, W_conv2, strides=[1, 1, 1, 1], padding='SAME') + b_conv2, name="h_conv2")
    h_pool2 = tf.nn.max_pool(h_conv2, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME', name="h_pool2")



    W_conv3 = tf.Variable(tf.truncated_normal([5, 5, 16, 32], stddev=0.1), name="W_conv3")
    b_conv3 = tf.Variable(tf.constant(0.1, shape=[32]), name="b_conv3")

    h_conv3 = tf.nn.relu(tf.nn.conv2d(h_pool2, W_conv3, strides=[1, 1, 1, 1], padding='SAME') + b_conv3, name="h_conv3")
    h_pool3 = tf.nn.max_pool(h_conv3, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME', name="h_pool3")


    W_conv4 = tf.Variable(tf.truncated_normal([5, 5, 32, 64], stddev=0.1), name="W_conv4")
    b_conv4 = tf.Variable(tf.constant(0.1, shape=[64]), name="b_conv4")

    h_conv4 = tf.nn.relu(tf.nn.conv2d(h_pool3, W_conv4, strides=[1, 1, 1, 1], padding='SAME') + b_conv4, name="h_conv4")
    h_pool4 = tf.nn.max_pool(h_conv4, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME', name="h_pool4")



    W_conv5 = tf.Variable(tf.truncated_normal([5, 5, 64, 128], stddev=0.1), name="W_conv5")
    b_conv5 = tf.Variable(tf.constant(0.1, shape=[128]), name="b_conv5")

    h_conv5 = tf.nn.relu(tf.nn.conv2d(h_pool4, W_conv5, strides=[1, 1, 1, 1], padding='SAME') + b_conv5, name="h_conv5")
    h_pool5 = tf.nn.max_pool(h_conv5, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME', name="h_pool5")


    W_conv6 = tf.Variable(tf.truncated_normal([5, 5, 128, 128], stddev=0.1), name="W_conv6")
    b_conv6 = tf.Variable(tf.constant(0.1, shape=[128]), name="b_conv6")

    h_conv6 = tf.nn.relu(tf.nn.conv2d(h_pool5, W_conv6, strides=[1, 1, 1, 1], padding='SAME') + b_conv6, name="h_conv6")
    h_pool6 = tf.nn.max_pool(h_conv6, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME', name="h_pool6")



    h_pool6_flat = tf.reshape(h_pool6, [-1, 4 * 4 * 128], name="h_pool6_flat")
    W_fc1 = tf.Variable(tf.truncated_normal([4 * 4 * 128, 1200], stddev=0.1), name="W_fc1")
    b_fc1 = tf.Variable(tf.constant(0.1, shape=[1200]), name="b_fc1")

    h_fc1 = tf.nn.relu(tf.matmul(h_pool6_flat, W_fc1) + b_fc1, name="h_fc1")
    keep_prob = tf.placeholder(tf.float32, name="keep_prob")
    h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob, name="h_fc1_drop")

    W_fc2 = tf.Variable(tf.truncated_normal([1200, 300], stddev=0.1), name="W_fc2")
    b_fc2 = tf.Variable(tf.constant(0.1, shape=[300]), name="b_fc2")




    y_conv = tf.add(tf.matmul(h_fc1_drop, W_fc2), b_fc2, name="y_conv")

    y_conv_shape = tf.reshape(y_conv, [batch_size, 10, 10, 3], name="y_conv_shape")

    # loss function
    loss = tf.reduce_mean(tf.pow(y_conv_shape - y_, 2), name="loss")
    train_step = tf.train.AdamOptimizer(alpha).minimize(loss)

    # tensorbord var
    loss_sum = tf.summary.scalar("loss", loss)
    merged = tf.summary.merge_all()


    sess = tf.Session()
    file_writer = tf.summary.FileWriter('./my_graph_CNN', sess.graph)
    sess.run(tf.global_variables_initializer())
    
    for i in range(0, num_of_batches*4):
        counter_batch = next_batch(counter_batch, path, num_of_batches)

        data_x, data_y = find_data_x_y()
        [_, curr_summary] = sess.run([train_step, merged], feed_dict={x: data_x, y_: data_y, keep_prob: 1.0})
        file_writer.add_summary(curr_summary, graph_counter)
        graph_counter += 1

        loss_eval = (loss.eval(session=sess, feed_dict={x: data_x, y_: data_y, keep_prob: 1.0})) ** 0.5
        loss_batch_list.append(loss_eval)
        loss_100_batch_avg_list.append(loss_eval)

        if i % 100 == 0 and i is not 0:
            print("Loss AVG for bathes {} to {} is {}".format(i-100, i, statistics.mean(loss_100_batch_avg_list)))
            loss_100_batch_avg_list.clear()


            # saves the model after every 100 batches
            path_to_save = "/home/shaynaor/Desktop/shay-study/year_3/semester_1/deep_lerning/Image_Inpainting/CNN/CNN_model/image_inpainting_cnn"
            saver = tf.train.Saver()
            saver.save(sess, path_to_save)



    print("FINISH THE TRAIN")
    file_writer.close()
    loss_avg = statistics.mean(loss_batch_list)
    print("Avg loss for all train images", loss_avg)
    write_losses_to_csv(loss_batch_list, loss_avg, "train")





def test():
    # reads the test images
    images_path = "/home/shaynaor/Downloads/image_inpainting_dataset/test_images_batches"
    path_to_restore = "/home/shaynaor/Desktop/shay-study/year_3/semester_1/deep_lerning/Image_Inpainting/CNN/CNN_model/image_inpainting_cnn"

    sess = tf.Session()
    saver = tf.train.Saver()

    saver.restore(sess, path_to_restore)

    graph = tf.get_default_graph()
    x = graph.get_tensor_by_name("x:0")
    y_ = graph.get_tensor_by_name("y_:0")
    y = graph.get_tensor_by_name("y_conv_shape:0")
    loss = graph.get_tensor_by_name("loss:0")
    keep_prob = graph.get_tensor_by_name("keep_prob:0")


    num_of_batches = 729
    counter_batch = 0
    batch_size = 50
    loss_batch_list = []
    pred_img_list = []

    for i in range(0, num_of_batches):
        counter_batch = next_batch(counter_batch, images_path, num_of_batches)

        # ---------------
        if i == 0:
            pic_counter = 0
            for img in img_holes_list:
                cv2.imwrite("/home/shaynaor/Desktop/shay-study/year_3/semester_1/deep_lerning/Image_Inpainting/CNN/evaluetion/before/{}.jpeg".format(pic_counter), img)
                pic_counter += 1
        # ---------------

        data_x, data_y = find_data_x_y()
        # ---------------
        if i == 0:
            pred = sess.run(y, feed_dict={x: data_x, y_: data_y, keep_prob: 1.0})
            pred_counter = 0
            for _ in img_holes_list:
                start_i, start_j = hole_indexes[pred_counter]
                img = img_holes_list[pred_counter].copy()
                img[start_j: start_j + size_hole, start_i: start_i + size_hole] = pred[pred_counter]
                pred_img_list.append(img)
                pred_counter += 1
        # ---------------


        loss_eval = (loss.eval(session=sess, feed_dict={x: data_x, y_: data_y, keep_prob: 1.0})) ** 0.5
        loss_batch_list.append(loss_eval)

        if i % 10 == 0:
            print("Loss AVG for batch number {} is {}".format(i, loss_eval))

    # ------------------
        if i == 0:
            pic_counter = 0
            for img in pred_img_list:
                cv2.imwrite("/home/shaynaor/Desktop/shay-study/year_3/semester_1/deep_lerning/Image_Inpainting/CNN/evaluetion/after/{}.jpeg".format(pic_counter), img)
                pic_counter += 1
    # ------------------

    print("FINISH THE TEST")
    loss_avg = statistics.mean(loss_batch_list)
    print("Avg loss for all test images", loss_avg)
    write_losses_to_csv(loss_batch_list, loss_avg, "test")


if __name__ == "__main__":
    train()
    test()