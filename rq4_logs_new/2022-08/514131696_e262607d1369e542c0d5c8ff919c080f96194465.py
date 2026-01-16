# A basic circular list structure 
# that can be initialized to an arbitrary size 
# and elements can be appended to the list using append() 
# Source: https://stackoverflow.com/questions/4151320/efficient-circular-buffer

class circularlist(object):
    def __init__(self, size, data = []):
        """Initialization of the circular list"""
        self.index = 0
        self.size = size
        self._data = list(data)[-size:]

    def append(self, value):
        """Append an element"""
        if len(self._data) == self.size:
            self._data[self.index] = value
        else:
            self._data.append(value)
        self.index = (self.index + 1) % self.size

    def __getitem__(self, key):
        """Get element by index, relative to the current index"""
        if len(self._data) == self.size:
            return(self._data[(key + self.index) % self.size])
        else:
            return(self._data[key])

    def __repr__(self):
        """Return string representation"""
        return (self._data[self.index:] + self._data[:self.index]).__repr__() + ' (' + str(len(self._data))+'/{} items)'.format(self.size)

c = circularlist(4)
c.append(1); print(c, c[0], c[-1])    #[1] (1/4 items)              1  1
c.append(2); print(c, c[0], c[-1])    #[1, 2] (2/4 items)           1  2
c.append(3); print(c, c[0], c[-1])    #[1, 2, 3] (3/4 items)        1  3
c.append(8); print(c, c[0], c[-1])    #[1, 2, 3, 8] (4/4 items)     1  8
c.append(10); print(c, c[0], c[-1])   #[2, 3, 8, 10] (4/4 items)    2  10
c.append(11); print(c, c[0], c[-1])   #[3, 8, 10, 11] (4/4 items)   3  11
d = circularlist(4, [1, 2, 3, 4, 5])  #[2, 3, 4, 5]