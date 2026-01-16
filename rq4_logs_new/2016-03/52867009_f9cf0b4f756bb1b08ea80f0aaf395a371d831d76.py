# Copyright (c) 2016, Dominika Salawa <vereena42@gmail.com>
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
# 
#     * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.
# 
#     * Neither the name of the <organization> nor the names of its
#       contributors may be used to endorse or promote products derived from this
#       software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import cv
import numpy
import matplotlib.pyplot as plt
import operator

from neural_network import NeuralNetworkModel

num_of_epochs=25

def unpickle(file):
    import cPickle
    fo = open(file, 'rb')
    dict = cPickle.load(fo)
    fo.close()
    return dict

dist=unpickle("data_batch_1")
data=numpy.array(dist['data'])
def rgb_image_from_array(arr, shape):
    color_range = reduce(operator.mul, shape, 1)
    r = arr[:color_range]
    g = arr[color_range:color_range*2]
    b = arr[color_range*2:color_range*3]

    new_shape = list(shape)
    new_shape.append(3)

    image = numpy.concatenate((r.reshape((color_range, 1)),
                            g.reshape((color_range, 1)),
                            b.reshape((color_range, 1))),
                           1).reshape(new_shape)
    return cv.fromarray(image, 2)


labels=numpy.array(dist['labels'])
dist=unpickle("data_batch_2")
data=numpy.concatenate((data,numpy.array(dist['data'])))
labels=numpy.concatenate((labels,numpy.array(dist['labels'])))
dist=unpickle("data_batch_3")
data=numpy.concatenate((data,numpy.array(dist['data'])))
labels=numpy.concatenate((labels,numpy.array(dist['labels'])))
dist=unpickle("data_batch_4")
data=numpy.concatenate((data,numpy.array(dist['data'])))
labels=numpy.concatenate((labels,numpy.array(dist['labels'])))
dist=unpickle("data_batch_5")
data=numpy.concatenate((data,numpy.array(dist['data'])))
labels=numpy.concatenate((labels,numpy.array(dist['labels'])))
dist=unpickle("test_batch")
test_data=numpy.array(dist['data'])
test_labels=numpy.array(dist['labels'])
names=unpickle("batches.meta")["label_names"]
print(names,len(names))

model=NeuralNetworkModel(True,[500],3072,50000,data/255.0,labels,10000,test_data/255.0,test_labels,True)
i=0
while i<num_of_epochs:
    model.train()
    print i+1
    print i+1,model.validate(1),model.validate(2)
    i+=1

model.search_for_best_and_worst()

worst=model.get_worst_images()
best=model.get_best_images()

worst_digits_figure = plt.figure()
worst_digits_plots = [worst_digits_figure.add_subplot(4, 5, i+1) for i in xrange(20)]
worst_digits_images = [worst_digits_plots[2*i].imshow(rgb_image_from_array(test_data[worst[i]], (32, 32))) for i in xrange(10)]
worst_digits_pies_plots = [worst_digits_plots[2*i+1].pie(model.get_percent(test_data[worst[i]]/255.0)[0].tolist(),labels=names) for i in xrange(10)]
worst_digits_figure.savefig("worst_cifar.png")

best_digits_figure = plt.figure()
best_digits_plots = [best_digits_figure.add_subplot(4, 5, i+1) for i in xrange(20)]
best_digits_images = [best_digits_plots[2*i].imshow(rgb_image_from_array(test_data[best[i]], (32, 32))) for i in xrange(10)]
best_digits_pies_plots = [best_digits_plots[2*i+1].pie(model.get_percent(test_data[best[i]]/255.0)[0].tolist(),labels=names) for i in xrange(10)]
best_digits_figure.savefig("best_cifar.png")