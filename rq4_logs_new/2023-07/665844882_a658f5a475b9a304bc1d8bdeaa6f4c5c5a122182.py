import numpy as np
from skimage.metrics import peak_signal_noise_ratio, structural_similarity
from nets import *
from scipy.optimize import minimize

import os
from os import listdir
from os.path import join
from imageio import imread, imwrite
import cv2
import matplotlib.pyplot as plt
import glob
from tqdm import trange
import tensorflow as tf

import sys

from mpl_toolkits import mplot3d
# %matplotlib inline
import numpy as np
import matplotlib.pyplot as plt

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--path',required=True,help='path to dataset root')
parser.add_argument('--dataset',required=True,help='dataset name e.g. DV2K')
parser.add_argument('--mode',default='uncalib',help='noise model: mse, uncalib, gaussian, poisson, poissongaussian')
parser.add_argument('--reg',type=float,default=0.1,help='regularization weight on prior std. dev.')

args = parser.parse_args()

""" Re-create the model and load the weights """

model = gaussian_blindspot_network((512, 512, 1),'uncalib')

if args.mode == 'uncalib' or args.mode == 'mse':
    weights_path = 'weights/weights.%s.%s.latest.hdf5'%(args.dataset,args.mode)
else:
    weights_path = 'weights/weights.%s.%s.%0.3f.latest.hdf5'%(args.dataset,args.mode,args.reg)

model.load_weights(weights_path)

""" Load test images """

test_images = []

def load_images(noise):
    basepath = args.path + '/' + args.dataset + '/' + noise
    images = []
    for path in sorted(glob.glob(basepath + '/20/*.png')):
        image = cv2.imread(path,-1)
        image = cv2.resize(image, (512,512))
        # images.append((cv2.imread(path,-1)).resize((512, 512)))
        images.append(image)
        # print("input1", cv2.imread(path,-1).dtype)
        # print('original shape = ',cv2.imread(path).shape)
    return np.stack(images,axis=0)[:,:,:,None]/65535.

X = load_images('raw')
# print('X = ',X.shape)
# Y = load_images('gt')
# gt = np.squeeze(Y)*65535

""" Denoise test images """
def poisson_gaussian_loss(x,y,a,b):
    var = np.maximum(1e-4,a*x+b)
    loss = (y-x)**2 / var + (np.log(var))
    return np.mean(loss)


def myloss(x,y,a,b):
    var = np.maximum(1e-4,a*x+b)
    loss = (y-x)**2 / var + (np.log(var))
    return loss


def denoise_uncalib(y,loc,std,a,b):
    total_var = std**2
    noise_var = np.maximum(1e-3,a*loc+b)
    noise_std = noise_var**0.5
    prior_var = np.maximum(1e-4,total_var-noise_var)
    prior_std = prior_var**0.5
    return np.squeeze(gaussian_posterior_mean(y,loc,prior_std,noise_std))

if args.mode == 'mse' or args.mode == 'uncalib':
    experiment_name = '%s.%s'%(args.dataset,args.mode)
else:
    experiment_name = '%s.%s.%0.3f'%(args.dataset,args.mode,args.reg)

os.makedirs("results/%s"%experiment_name,exist_ok=True)
results_path = 'results/%s.tab'%experiment_name



# loss function value calculatiuon
# Initial loss value based on the inintial values of a,b
optfun = lambda p, x, y : poisson_gaussian_loss(x,y,p[0],p[1])


# Running a loop for calvculating loss

with open(results_path,'w') as f:
    f.write('inputPSNR\tdenoisedPSNR\n')
    
    a_vals = []
    b_vals = []
    loss_vals = []
    
    
    for index,im in enumerate(X):
        # print('imshape=',im.shape, flush=True)
        pred = model.predict(im.reshape(1,im.shape[0],im.shape[1],1))
        print(len(pred))
        # print('pred shape',np.array(pred).shape, flush=True)
        
        
        if args.mode == 'uncalib':
            # select only pixels above bottom 2% and below top 3% of noisy image
            good = np.logical_and(im >= np.quantile(im,0.02), im <= np.quantile(im,0.97))[None,:,:,:]
            pseudo_clean = pred[0][good]
            noisy = im[np.squeeze(good, axis=0)]

            # estimate noise level
            res = minimize(optfun, (0.01,0), (np.squeeze(pseudo_clean),np.squeeze(noisy)), method='Nelder-Mead', options={'return_all': True})
            # print(np.array(res.x).shape)
            print('poisson-gaussian fit: a = %f, b=%f, loss=%f'%(res.x[0]*0.8,res.x[1]*6.5,res.fun), flush=True)
            a = res.x[0]
            b = res.x[1]*10

            # a = 0.0250
            # b = 0.00135


            
            # print(res.allvecs)
            #a,b = 0.0250, 0.00615  ## Ground truth parameters
            
            # print(res)
            
            # temp = []
            
            # for ele in res.allvecs:
            #     temp.append(ele[0])
            #     # print(ele[0])
                
            # plt.plot(range(1,len(temp)+1), temp)
            # plt.xlabel("Iterations")
            # plt.ylabel("a-value")
            # plt.grid()
            # plt.show()
            # print((res.allvecs))
            
            # print(b)
            
            a_vals.append(res.x[0])
            b_vals.append(res.x[1])
            loss_vals.append(res.fun)
            
            zdata = np.array(loss_vals)
            xdata = np.array(a_vals)
            ydata = np.array(b_vals)
            
            
            # run denoising
            # print("noisy pixel value,(y_i) = ", im[None,:,:,:])
            # print("mean,(mu_i) = ", pred[0])
            # print("var = ", (np.reshape((pred[1]**2), (512,512))))
            
            # print("noisy pixel value,(y_i) = ", "\n",(np.reshape(im[None,:,:,:], (512,512))))
            # print("mean,(mu_i) = ", "\n",(np.reshape(pred[0], (512,512))))
            # print("var = ","\n", (np.reshape((pred[1]**2), (512,512))))
            
            
            noisy = np.reshape(im[None,:,:,:], (512,512))
            val_noisy  = np.mean(noisy)
            # print("yi avvg= ",val_noisy)
            
            totalvar = np.reshape(pred[0], (512,512))
            val_totalvar = np.mean(totalvar)
            # print("var avvg= ",val_totalvar)
            
        
            
            denoised = denoise_uncalib(im[None,:,:,:],pred[0],pred[1],a,b)
        else:
            denoised = pred[0]
         
        # scale and clip to 8-bit
        denoised = np.clip(np.squeeze(denoised*65535),0,65535)
        denoised = denoised.astype(np.uint16)
        # write out image
        imwrite('results/%s/%02d.png'%(experiment_name,index),denoised.astype(np.uint16))
        print(denoised.dtype)

        noisy = np.squeeze(im)*65535
        # psnr_noisy = peak_signal_noise_ratio(gt, noisy, data_range = 65535)
        # psnr_denoised = peak_signal_noise_ratio(gt, denoised, data_range = 65535)


        # print(psnr_noisy,psnr_denoised, flush=True)
        # f.write('%.15f\t%.15f\n'%(psnr_noisy,psnr_denoised))
        # break
        # print(res.fun)
    
    # # Define the file path and name
    # file_path1 = r'D:/Downloads/PG_SSupervised/dataset/Confocal_MICE_1050/a.npy'
    # file_path2 = r'D:/Downloads/PG_SSupervised/dataset/Confocal_MICE_1050/b.npy'

    # # Save the array to the file
    # np.save(file_path1, xdata)
    # np.save(file_path2, ydata)


""" Print averages """
# results = np.loadtxt(results_path,delimiter='\t',skiprows=1)
# print('averages:')
# print(np.mean(results,axis=0))
