import numpy
import scipy

class Projection(object):    
    """
    A set of connections from one sheet to another. This is an abstract class.
    """
    
    def __init__(self, name, source, target, strength,delay):
        """
        Each Projection has a source sheet and target sheet.
        
        Parameters
        ----------
        source : Sheet
                 The sheet from which projection passes rates.

        target : Sheet
                 The sheet to which projection passes rates.
                 
        strength : float
                The strength of the projection.
        
        delay : float
                The delay of the projection
        """
        self.name = name
        self.source = source
        self.target = target
        self.strength = strength
        self.delay = delay
        assert delay > 0, "ERROR: zero delay projections are not allowed"
        self.target._register_in_projection(self)
        self.source._register_out_projection(self)
        self.activity = None
        
    
    def activate(self):
        """
        This returns a matrix of the same size as the target sheet, which corresponds to the contributions from this projections to the individual neurons.
        """
        raise NotImplementedError
        
    

class ConvolutionalProjection(Projection):    
    """
    A projection which has only one set of connections (connection kernel) which are assumed to be the same for all target neurons, except being centered on their position.
    This means that the output of ConvolutionalProjection is the spatial convolution of this connection kernel with the activities of the source Sheet.
    
    Note that the connection_kernel will be normalized so that the sum of it's absolute values is 1.
    """
    def __init__(self,name,source, target, strength,delay,connection_kernel):
        """
        Each Projection has a source sheet and target sheet.
        
        Parameters
        ----------
        source : Sheet
                 The sheet from which projection passes rates.

        target : Sheet
                 The sheet to which projection passes rates.
                 
        strength : float
                 The strength of the projection.
        """
        Projection.__init__(self, name,source, target, strength,delay)
        self.connection_kernel = connection_kernel / numpy.sum(numpy.abs(connection_kernel))
        
        size_diff = self.source.radius - self.target.radius
        assert size_diff >=0 , "ERROR: The radius of source sheet of ConvolutionalProjection has to be larger than target"
        
        assert numpy.shape(connection_kernel)[0] % 2 == 1, "ERROR: initial kernel for ConvolutionalProjection has to have odd radius"
        
        #lets calculate edge correction factors:
        self.rad = int((numpy.shape(connection_kernel)[0]-1)/2)
        target_dim = self.target.radius*2
        source_dim = self.source.radius*2
        cfs = [[self.connection_kernel.copy()[max(0,self.rad-(size_diff+j)):2*self.rad+1-max(0,(size_diff+j+1+self.rad)-source_dim),max(0,self.rad-(size_diff+i)):2*self.rad+1-max(0,(size_diff+i+1+self.rad)-source_dim)] for i in xrange(target_dim)] for j in xrange(target_dim)]
        self.corr_factors = numpy.array([[1/numpy.sum(numpy.abs(b)) for b in a] for a in cfs])
        
    def activate(self):
        """
        This returns a matrix of the same size as the target sheet, which corresponds to the contributions from this projections to the individual neurons.
        """
        size_diff = self.source.radius - self.target.radius
        resp = scipy.signal.fftconvolve(self.source.get_activity(self.delay),self.connection_kernel, mode='same')
        if size_diff != 0:
            resp = resp[size_diff:-size_diff,size_diff:-size_diff]
        assert numpy.shape(resp) == (2*self.target.radius,2*self.target.radius), "ERROR: The size of calculated projection respone is " + str(numpy.shape(resp)) + "units, while the size of target sheet is " + str((2*self.target.radius,2*self.target.radius)) + " units"
        self.activity = numpy.multiply(resp,self.corr_factors) * self.strength



class ConnetcionFieldProjection(Projection):    
    """
    A projection which has can have different connection field for each target neuron, which are assumed to be centered on the target's neuron position.
    This means that the output of ConvolutionalProjection is the spatial convolution of this connection kernel with the activities of the source Sheet.
    
    Note that all connection fields, including the border ones, will be normalized so that the sum of their absolute values is 1.    
    """
    def __init__(self,name,source, target, strength,delay,initial_connection_kernel):
        """
        Each Projection has a source sheet and target sheet.
        
        Parameters
        ----------
        source : Sheet
                 The sheet from which projection passes rates.

        target : Sheet
                 The sheet to which projection passes rates.
                 
        strength : float
                 The strength of the projection.
        """
        Projection.__init__(self, name,source, target, strength,delay)
        assert numpy.shape(initial_connection_kernel)[0] % 2 == 1, "ERROR: initial kernel for ConnetionFieldProjection has to have odd radius"
        assert numpy.all(numpy.shape(initial_connection_kernel) < (2*self.target.radius,2*self.target.radius))
        
        size_diff = self.source.radius - self.target.radius
        assert size_diff >=0 , "ERROR: The radius of source sheet of ConvolutionalProjection has to be larger than target"
        
        
        self.rad = int((numpy.shape(initial_connection_kernel)[0]-1)/2)
        source_dim = self.source.radius*2
        target_dim = self.target.radius*2
        initial_connection_kernel = initial_connection_kernel/numpy.sum(numpy.abs(initial_connection_kernel))
        self.cfs = [[initial_connection_kernel.copy()[max(0,self.rad-(size_diff+j)):2*self.rad+1-max(0,(size_diff+j+1+self.rad)-source_dim),max(0,self.rad-(size_diff+i)):2*self.rad+1-max(0,(size_diff+i+1+self.rad)-source_dim)] for i in xrange(target_dim)] for j in xrange(target_dim)]
        
        
        # make sure we created correct sizes
        for i in xrange(self.target.radius*2):
            for j in xrange(self.target.radius*2):
                assert numpy.all(numpy.shape(self.cfs[i][j]) <= numpy.shape(initial_connection_kernel)) and numpy.all(numpy.shape(self.cfs[i][j]) > (self.rad,self.rad)) , "ERROR: Connection field created at location " + str(i) + "," + str(j) + " that has size: " + str(numpy.shape(self.cfs[i][j])) + " while template size is:" + str(numpy.shape(initial_connection_kernel))
                # flatten and normalize 
                self.cfs[i][j] = self.cfs[i][j].flatten()  / numpy.sum(numpy.abs(self.cfs[i][j]))
                
        self.activity = numpy.zeros((target_dim,target_dim))
    
    def activate(self):
        """
        This returns a matrix of the same size as the target sheet, which corresponds to the contributions from this projections to the individual neurons.
        """
        sa = self.source.get_activity(self.delay)
        size_diff = self.source.radius - self.target.radius
        
        for i in xrange(self.target.radius*2):
            for j in xrange(self.target.radius*2):
                self.activity[i][j] = self.cfs[i][j].dot(sa[max(size_diff+i-self.rad,0):size_diff+i+self.rad+1,max(size_diff+j-self.rad,0):size_diff+j+self.rad+1].ravel())
                
        self.activity *= self.strength
        
    def get_cf(self,posx,posy):
        return cfs[posx,posy]


class FastConnetcionFieldProjection(Projection):    
    """
    A projection which has can have different connection field for each target neuron, which are assumed to be centered on the target's neuron position.
    This means that the output of ConvolutionalProjection is the spatial convolution of this connection kernel with the activities of the source Sheet.
    
    Note that all connection fields, including the border ones, will be normalized so that the sum of their absolute values is 1.    
    """
    def __init__(self,name,source, target, strength,delay,initial_connection_kernel):
        """
        Each Projection has a source sheet and target sheet.
        
        Parameters
        ----------
        source : Sheet
                 The sheet from which projection passes rates.

        target : Sheet
                 The sheet to which projection passes rates.
                 
        strength : float
                 The strength of the projection.
                 
        inititial_connection_kernel: ndarray
                 The initial connection kernel. 
        """
        Projection.__init__(self, name,source, target, strength,delay)
        assert numpy.shape(initial_connection_kernel)[0] % 2 == 1, "ERROR: initial kernel for FastConnetcionFieldProjection has to have odd radius"
        assert numpy.all(numpy.shape(initial_connection_kernel) <= (2*self.source.radius,2*self.source.radius))
        assert numpy.sum(numpy.abs(initial_connection_kernel)) != 0 , "ERROR: initial kernel for FastConnetcionFieldProjection has to have non zero L1 norm"
        size_diff = self.source.radius - self.target.radius 
        assert size_diff >=0 , "ERROR: The radius of source sheet of ConvolutionalProjection has to be larger than target"
        self.rad = int((numpy.shape(initial_connection_kernel)[0]-1)/2)
        source_dim = self.source.radius*2
        target_dim = self.target.radius*2
        self.cfs = numpy.zeros((target_dim*target_dim,source_dim*source_dim))
        
        padding = max(0,self.rad-size_diff)
        tmp = numpy.zeros((source_dim+padding*2,source_dim+padding*2))
        offset = size_diff + padding
        
        for i in xrange(target_dim):
            for j in xrange(target_dim):
                tmp *=0
                tmp[i+offset-self.rad:i+offset+self.rad+1,j+offset-self.rad:j+offset+self.rad+1] = initial_connection_kernel
                self.cfs[i*target_dim+j,:] = tmp[padding:source_dim+padding,padding:source_dim+padding].copy().flatten() / numpy.sum(numpy.abs(tmp[padding:source_dim+padding,padding:source_dim+padding]))
        self.activity = numpy.zeros((target_dim,target_dim))
    
    def activate(self):
        """
        This returns a matrix of the same size as the target sheet, which corresponds to the contributions from this projections to the individual neurons.
        """
        sa = self.source.get_activity(self.delay).ravel()
        size_diff = self.source.radius - self.target.radius
        self.activity = self.strength * numpy.reshape(numpy.dot(self.cfs,sa),(self.target.radius*2,self.target.radius*2))
        
    def get_cf(self,posx,posy):
        size_diff = self.source.radius - self.target.radius
        
        return numpy.reshape(self.cfs[posx*self.target.radius*2+posy,:],(self.source.radius*2,self.source.radius*2))
        #[posx+size_diff-self.rad,posx+size_diff+self.rad+1][posy+size_diff-self.rad,posy+size_diff+self.rad+1]
    
    
    
    
    
    