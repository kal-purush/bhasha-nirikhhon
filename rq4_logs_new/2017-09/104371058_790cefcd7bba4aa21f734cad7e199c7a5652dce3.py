import numpy as np
from PIL import Image
from sklearn.feature_extraction import image
from skimage.util.shape import view_as_windows


class Block():
    def __init__(self):
        self.block_dict = {}


im = Image.open("person.jpg")
im = im.crop((20,20,580,443))

target_width = 2
stride = target_width//2

width,height = im.size
print(width)
print(height)
matrix = np.zeros([width,height])
for y in range(height):
    for x in range(width):
        matrix[x,y] = im.getpixel((x,y))[0]

#blocks = image.extract_patches_2d(matrix,[stride,stride])
#print(blocks[0].sum())
#print(blocks[0])
block_list = []
#print(blocks.shape)
'''
for ind,block in enumerate(blocks):
    ind_block = Block()
    #m = 1/(stride**2)*ind_block.sum()
'''


#print(matrix[0:5,0:5])

#B = view_as_windows(matrix,(target_width,target_width), step=stride)

A = np.arange(4*4).reshape(4,4)
B = view_as_windows(A,(2,2), step=1)
print(A)
x_win_dim = B.shape[0]
y_win_dim = B.shape[1]

print(B[0,0])
print(B.shape)
print(B[0,1])
m_mat = np.zeros([x_win_dim,y_win_dim])
l_mat = np.zeros([x_win_dim,y_win_dim])
for y_win in range(y_win_dim):
    for x_win in range(x_win_dim):
        m_mat[x_win, y_win] = 1 / (target_width ** 2) * B[x_win,y_win].sum()
        l_mat[x_win, y_win] = np.amax(B[x_win,y_win])
print(m_mat)
print(l_mat)