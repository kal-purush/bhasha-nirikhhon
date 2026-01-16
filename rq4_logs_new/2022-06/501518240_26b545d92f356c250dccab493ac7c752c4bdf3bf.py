from astropy.convolution import Ring2DKernel

kernel = Ring2DKernel(1, 1)
print(kernel.array)