import numpy as np

data = [[[1, 0, 0, 0, 0, 0, 0],
        [1, 0.5, 0, 0, 0, 1.63, 0],
        [1, 0.25, 0.25, 0, 0.8, 0.8, 0]]]

#data = [[np.random.random(7) for n in range(4)]]

# add zeros for the extra dims to lists in data
extra_dims = 6
for particle in data[0]:
    for n in range(extra_dims):
        particle.append(0)
    
# convert data lists to array, then make an array of zeros the same size
arr = np.array(data)
arr_zeros = np.zeros(np.shape(arr))

# define max time steps, number of particles and number of dimensions
t_max = 4
particles = np.shape(arr)[1]
dimensions = np.shape(arr)[2]

print(np.shape(arr))

t = 0
while t <= t_max:
    arr = np.concatenate([arr, arr_zeros])
    arr[t + 1, 1, 2] += t
    t += 1

print(arr)