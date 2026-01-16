import numpy as np
import tensorflow as tf
from numpy import genfromtxt
import csv
import pprint
import os


def read():
    points = genfromtxt('/opt/awsMachineData.csv', delimiter=',')
    col_mean = points.mean(axis=0)
    col_sdev =  points.std(axis=0)
    points = (points-col_mean)
    points = points/col_sdev 
    points = np.delete(points, [2,6,8,9,10,11,14,18,22,26,30,34,38,42,46], axis=1)
    col_mean = np.nanmean(points, axis=0)
    inds = np.where(np.isnan(points))
    points[inds] = np.take(col_mean, inds[1])
    return points

def input_fn():
    points = read()
    return tf.train.limit_epochs(tf.convert_to_tensor(points, dtype=tf.float32), num_epochs=1)

def main(_):
    
    num_clusters = 6
    sess = tf.InteractiveSession()
    kmeans = tf.contrib.factorization.KMeansClustering(num_clusters=num_clusters, use_mini_batch=False)
    
    num_iterations = 2
    previous_centers = None
    for _ in range(num_iterations):
        kmeans.train(input_fn)
        cluster_centers = kmeans.cluster_centers()
        previous_centers = cluster_centers
    
    cluster_indices = list(kmeans.predict_cluster_index(input_fn))
    i = 1
    tags = {}
    with open('/opt/awsMachineTag.csv') as f:
        csv_reader = csv.reader(f, delimiter=',')
        for row in csv_reader:
            tags[i] = row[0]
            i = i + 1
    
    points = read()
    cluster_content = {}
    for i, point in enumerate(points):
        cluster_index = cluster_indices[i]
        machineType = tags[i+1]
        if machineType in cluster_content[cluster_index]:
            cluster_content[cluster_index][machineType] = cluster_content[cluster_index][machineType] + 1
        else:
            cluster_content[cluster_index][machineType] = 1
    
    #write to file
    done = []
    print(cluster_content)
    with open('/opt/clusterContent.txt',  mode='w') as f:
        for i in cluster_indices:
            cluster_index = cluster_indices[i]
            if cluster_index not in done:
                f.write("%s\n" % cluster_content[cluster_index])
                done.append(cluster_index)
    
    export_path_base = '/opt'
    export_path = os.path.join(tf.compat.as_bytes(export_path_base), tf.compat.as_bytes(str('1.2')))
    print(export_path)
    builder = tf.saved_model.builder.SavedModelBuilder(export_path)
    
    tensor_info_x = tf.saved_model.utils.build_tensor_info(tf.convert_to_tensor(points[0], dtype=tf.float32))
    tensor_info_y = tf.saved_model.utils.build_tensor_info(tf.convert_to_tensor(cluster_indices[0]))

    prediction_signature = (
      tf.saved_model.signature_def_utils.build_signature_def(
          inputs={'metrics': tensor_info_x},
          outputs={'clusterId': tensor_info_y},
          method_name=tf.saved_model.signature_constants.PREDICT_METHOD_NAME))
    
    builder.add_meta_graph_and_variables(
        sess, [tf.saved_model.tag_constants.SERVING],
        signature_def_map={'predict_cluster': prediction_signature,})

    # export the model
    builder.save(as_text=True)
    sess.close()
    
if __name__ == '__main__':
    tf.app.run()