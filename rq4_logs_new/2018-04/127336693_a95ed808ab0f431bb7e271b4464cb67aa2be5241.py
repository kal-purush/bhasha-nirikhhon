import tensorflow as tf
import time
from util import tf_help
from collections import namedtuple

writer = tf_help.crete_file_writer(__file__)

sess = tf.Session()

with tf.name_scope("Dataset_and_Iterator"):

    my_data = [
        [1, 2],
        [3, 4],
        [5, 6],
        [7, 8],
    ]

    slices = tf.data.Dataset.from_tensor_slices(my_data)

    print(slices)  # <TensorSliceDataset shapes: (2,), types: tf.int32>
    my_iter = slices.make_one_shot_iterator()
    print(my_iter)  # <tensorflow.python.data.ops.iterator_ops.Iterator object at 0x7fc7930c0048>
    next_item = my_iter.get_next(name="next_item")
    print(next_item)  # Tensor("IteratorGetNext:0", shape=(2,), dtype=int32)

print(next_item.eval(session=sess))



with tf.name_scope("Initialize_Iterator"):
    print("Initialize Iterator")
    r = tf.random_uniform(shape=(3, 4), minval=10, maxval=50, dtype=tf.int32, name="r")
    # print(r.eval(session=sess))
    slices = tf.data.Dataset.from_tensor_slices(r)
    my_iter = slices.make_initializable_iterator(shared_name="my_iter")
    print(my_iter)
    next_item = my_iter.get_next(name="next_item")
    initializer = my_iter.initializer
    print(initializer, type(initializer))

    sess.run(initializer)

while True:
    try:
        print(next_item.eval(session=sess))
    except tf.errors.OutOfRangeError:
        break



writer.add_graph(tf.get_default_graph())
time.sleep(1) #wait for writer writes events to disk
print("done")