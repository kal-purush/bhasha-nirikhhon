import tensorflow as tf
import syft as sf 

hook = sy.TensorFlowHook(tf)

x = tf.constant([1., 2., 3., 4., 5.])
sub = x - x
print(sub) # output ([0., 0., 0., 0., 0.])

tom = sy.VirtualWorker(hook, id="tom")
x = tf.constant([5., 4., 3., 2., 1.])
y = tf.constant([1., 1., 1., 1., 1.])

x_ptr = x.send(tom)
y_ptr = y.send(tom)

sub_ptr = x_ptr - y_ptr
print(sub_ptr) #output ([4., 3., 2., 1., 0.])

x = torch.tensor([1,2,3,4,5.], requires_grad=True).send(jim)
y = torch.tensor([1,1,1,1,1.], requires_grad=True).send(jim)

sub_z=  tf.subtract(x,y)
sub_z=sub_z.get()
print(sub_z) #output ([0., 1., 2., 3., 4.])