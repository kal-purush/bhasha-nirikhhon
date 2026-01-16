#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, re, time, math
import subprocess

import h5py
import pickle

import numpy as np
import scipy as sp

import tqdm
from tqdm import tqdm
import shutil
import pathlib
from pathlib import Path, PurePosixPath

import numba
from numba import jit,njit

import datetime
import timeit

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import cmocean, colorcet, cmasher

import PIL

'''
========================================================================

Dynamic Mode Decomposition (DMD)

========================================================================
'''

# utilities
# ======================================================================

def format_time_string(tsec):
    '''
    format seconds as dd:hh:mm:ss
    '''
    m, s = divmod(tsec,60)
    h, m = divmod(m,60)
    d, h = divmod(h,24)
    time_str = '%dd:%dh:%02dm:%02ds'%(d,h,m,s)
    return time_str

def even_print(label, output, **kwargs):
    '''
    print/return a fixed width message
    '''
    terminal_width = kwargs.get('terminal_width',72)
    s              = kwargs.get('s',False) ## return string
    
    ndots = (terminal_width-2) - len(label) - len(output)
    text = label+' '+ndots*'.'+' '+output
    if s:
        return text
    else:
        #sys.stdout.write(text)
        print(text)
        return

def fig_trim_x(fig, list_of_axes, **kwargs):
    '''
    trims the figure in (x) / width dimension
    - typical use case : single equal aspect figure needs to be scooted / trimmed
    '''
    
    offset_px = kwargs.get('offset_px',10)
    dpi_out   = kwargs.get('dpi',None) ## this is used to make sure OUTPUT png px dims are divisible by N
    if (dpi_out is None):
        dpi_out = fig.dpi
    
    fig_px_x, fig_px_y = fig.get_size_inches()*dpi_out
    #print('fig size px : %i %i'%(fig_px_x, fig_px_y))
    transFigInv = fig.transFigure.inverted()
    
    w = fig.get_figwidth()
    h = fig.get_figheight()
    #print('w, h : %0.2f %0.2f'%(w, h))
    
    nax = len(list_of_axes)
    
    ax_tb_pct = np.zeros( (nax,4) , dtype=np.float64 )
    ax_tb_px  = np.zeros( (nax,4) , dtype=np.float64 )
    for ai, axis in enumerate(list_of_axes):
        
        ## percent values of the axis tightbox
        x0, y0, dx, dy  = axis.get_position().bounds
        #print('x0, y0, dx, dy : %0.2f %0.2f %0.2f %0.2f'%(x0, y0, dx, dy))
        ax_tb_pct[ai,:] = np.array([x0, y0, dx, dy])
        
        ## pixel values of the axis tightbox
        x0, y0, dx, dy = axis.get_tightbbox(fig.canvas.get_renderer(), call_axes_locator=True).bounds
        #print('x0, y0, dx, dy : %0.2f %0.2f %0.2f %0.2f'%(x0, y0, dx, dy))
        axis_tb_px  = np.array([x0, y0, dx, dy])
        axis_tb_px *= (dpi_out/fig.dpi) ## scale by dpi ratio [png:screen]
        ax_tb_px[ai,:] = axis_tb_px
    
    ## current width of (untrimmed) margins (in [x])
    marg_R_px = fig_px_x - (ax_tb_px[:,0] + ax_tb_px[:,2]).max()
    marg_L_px = ax_tb_px[:,0].min()
    #print('marg_L_px : %0.2f'%(marg_L_px,))
    #print('marg_R_px : %0.2f'%(marg_R_px,))
    
    ## n pixels to move all axes left by (fig canvas is 'trimmed' from right)
    x_shift_px = marg_L_px - offset_px
    #print('x_shift_px : %0.2f'%(x_shift_px,))
    
    ## get new canvas size
    ## make sure height in px divisible by N (important for video encoding)
    px_base = 8
    w_px    = fig_px_x - marg_L_px - marg_R_px + 2*offset_px
    w_px    = math.ceil(w_px/px_base)*px_base
    #w_px   += 1*px_base ## maybe helpful in case labels have \infty etc., where get_position() slightly underestimates
    h_px    = fig_px_y
    #print('w_px, h_px : %0.2f %0.2f'%(w_px, h_px))
    w_inch  = w_px / dpi_out
    h_inch  = h_px / dpi_out
    
    ## get shifted axis bound values
    for ai, axis in enumerate(list_of_axes):
        x0, y0, dx, dy = axis.get_position().bounds
        x0n = x0 - x_shift_px/fig_px_x
        y0n = y0
        dxn = dx
        dyn = dy
        ax_tb_pct[ai,:] = np.array([x0n, y0n, dxn, dyn])
        #print('x0n, y0n, dxn, dyn : %0.4f %0.4f %0.4f %0.4f'%(x0n, y0n, dxn, dyn))
    
    ## resize canvas
    fig.set_size_inches( w_inch, h_inch, forward=True )
    fig_px_x, fig_px_y = fig.get_size_inches()*dpi_out
    w_adj = fig.get_figwidth()
    h_adj = fig.get_figheight()
    #print('w_adj, h_adj : %0.2f %0.2f'%(w_adj, h_adj))
    #print('w_adj, h_adj : %0.2f %0.2f'%(w_adj*dpi_out, h_adj*dpi_out))
    
    ## do shift
    for ai, axis in enumerate(list_of_axes):
        x0n, y0n, dxn, dyn = ax_tb_pct[ai,:]
        axis.set_position( [ x0n*(w/w_adj) , y0n , dxn*(w/w_adj) , dyn ] )
    
    return

# Dynamic Mode Decomposition (DMD)
# ======================================================================

def dmd_data_loader_ns3d(fn_cgd):
    '''
    retrieve data from an NS3D result HDF5 (EAS4, RGD, CGD, etc.) in preparation for DMD analysis
    '''
    raise NotImplementedError('dmd_data_loader_ns3d() not yet implemented')
    data = np.zeros((6,7), dtype=np.float32) ## dummy
    extra = { 'U_inf':1, 'lchar':1 } ## dummy
    return data, extra

def dmd_data_loader_1d(fn_h5, **kwargs):
    '''
    retrieve data from a 1D chaotic oscillator result
    '''
    raise NotImplementedError('dmd_data_loader_1d() not yet implemented')
    data = np.zeros((6,7), dtype=np.float32) ## dummy
    extra = { 'U_inf':1, 'lchar':1 } ## dummy
    return data, extra

def dmd_data_loader_lbm(fn_h5, scalar='umag', **kwargs):
    '''
    retrieve data from a 2D LBM simulation result & prepare it for DMD analysis
    -----
    - returned 'data' is a numpy array with shape (ndof,nt)
    - returned 'extra' is a dict of any extra information (for plotting, etc)
    '''
    
    verbose = kwargs.get('verbose',True)
    ti_min  = kwargs.get('ti_min',None)
    ti_max  = kwargs.get('ti_max',None)
    xi_min  = kwargs.get('xi_min',None)
    xi_max  = kwargs.get('xi_max',None)
    yi_min  = kwargs.get('yi_min',None)
    yi_max  = kwargs.get('yi_max',None)
    
    if verbose: print('\n'+'dmd_data_loader_lbm()')
    if verbose: print(72*'-')
    
    fn_h5 = Path(fn_h5)
    fn_h5 = Path(os.path.relpath(fn_h5,Path()))
    if not fn_h5.exists():
        raise FileNotFoundError('%s not found!'%fn_h5)
    if verbose: even_print('%s'%(fn_h5,), '%0.1f [GB]'%(os.path.getsize(fn_h5)/1024**3,))
    
    # === open HDF5 file & copy data
    
    with h5py.File(fn_h5,'r') as f1:
        
        U_inf    = f1['U_inf'][0]
        lchar    = f1['lchar'][0]
        Re       = f1['Re'][0]
        nu_inf   = f1['nu_inf'][0]
        x        = np.copy( f1['x'][()] )
        y        = np.copy( f1['y'][()] )
        obstacle = np.copy( f1['obstacle'][()] )
        
        nx = x.shape[0]
        ny = y.shape[0]
        
        obstacle_img = np.zeros((nx,ny,4), dtype=np.uint8 ) ## transparent
        for i in range(nx):
            for j in range(ny):
                if obstacle[i,j]:
                    obstacle_img[i,j,:] = [80,80,80,255] ## gray, opaque
        
        ## check that Re=U_inf*lchar/nu_inf
        if not np.isclose(Re, (U_inf*lchar/nu_inf), rtol=1e-14, atol=1e-14):
            raise ValueError('Re!=U_inf*lchar/nu_inf')
        
        ## report flow parameters
        if verbose: even_print('lchar'     , '%i [lattice units]'%(lchar,))
        if verbose: even_print('U_inf'     , '%0.5e [lattice units]'%(U_inf,))
        if verbose: even_print('Re'        , '%0.1f'%(Re,))
        if verbose: even_print('nu_inf'    , '%0.5e'%(nu_inf,))
        if verbose: even_print('nx (full)' , '%i'%(nx,))
        if verbose: even_print('ny (full)' , '%i'%(ny,))
        
        ## get ts_XXXXXX dataset info
        dset_names = list(f1.keys())
        tss = sorted([ dss for dss in dset_names if re.match(r'ts_\d',dss) ])
        ti = np.array( sorted([ int(re.findall(r'[0-9]+', tss_)[0]) for tss_ in tss ]), dtype=np.int32 )
        if not np.all( np.diff(ti)==1 ):
            raise AssertionError('ti not constant')
        nt = ti.shape[0]
        if verbose: even_print('nt (full)', '%i'%(nt,))
        
        if verbose: print(72*'-')
        
        ## make sure that scalar to be used is valid
        if (scalar=='umag'):
            if verbose: even_print('dmd scalar', scalar)
        else:
            print(">>> scalar='%s' not a valid choice (or not yet implemented)")
            print(">>> options are: 'umag'")
            raise NotImplementedError
        
        # === reduce bounds of data for analysis
        
        if ti_min is None:
            ti_min = ti.min()
        else:
            ti_min = max(ti_min,ti.min())
        
        if ti_max is None:
            ti_max = ti.max()
        else:
            ti_max = min(ti_max,ti.max())
        
        if verbose: even_print('ti_min', '%i'%(ti_min,))
        if verbose: even_print('ti_max', '%i'%(ti_max,))
        nt = ti_max - ti_min + 1
        
        if xi_min is None:
            xi_min = 0
        else:
            xi_min = max(xi_min,0)
        
        if xi_max is None:
            xi_max = nx
        else:
            xi_max = min(xi_max,nx)
        
        if verbose: even_print('xi_min', '%i'%(xi_min,))
        if verbose: even_print('xi_max', '%i'%(xi_max,))
        
        x = np.copy(x[xi_min:xi_max+1])
        nx = x.shape[0]
        
        if yi_min is None:
            yi_min = 0
        else:
            yi_min = max(yi_min,0)
        
        if yi_max is None:
            yi_max = ny
        else:
            yi_max = min(yi_max,ny)
        
        if verbose: even_print('yi_min', '%i'%(yi_min,))
        if verbose: even_print('yi_max', '%i'%(yi_max,))
        y = np.copy(y[yi_min:yi_max+1])
        ny = y.shape[0]
        
        ## crop obstacle img
        obstacle_img = np.copy(obstacle_img[xi_min:xi_max+1,yi_min:yi_max+1,:])
        
        ## check that nx,ny,nt are all >0
        if not nx>0:
            raise ValueError('nx=%i (not >0)'%(nx,))
        if not ny>0:
            raise ValueError('ny=%i (not >0)'%(ny,))
        if not nt>0:
            raise ValueError('nt=%i (not >0)'%(nt,))
        
        ## report the bounds of the cropped data to be used
        if verbose: print(72*'-')
        if verbose: even_print('nx', '%i'%(nx,))
        if verbose: even_print('ny', '%i'%(ny,))
        if verbose: even_print('nt', '%i'%(nt,))
        if verbose: print(72*'-')
        
        # === copy data to memory (data read)
        
        # if reading multiple scalars, could use structured array, but for now just
        #   read single scalar
        
        ## names   = [ 'u','v','umag' ]
        ## formats = [ np.float32 for n in names ]
        ## 
        ## ## 4D [scalar][x,y,t] structured array
        ## data = np.zeros(shape=(nx,ny,nt), dtype={'names':names, 'formats':formats})
        
        data = np.zeros(shape=(nx,ny,nt), dtype=np.float32)
        
        tiii = -1
        if verbose: progress_bar = tqdm(total=nt, ncols=100, desc='data read', leave=False, file=sys.stdout)
        for tii in range(ti_min,ti_max+1):
            tiii += 1 ## index in structured array
            ##
            u = np.copy( f1['ts_%06d/u'%ti[tii]][xi_min:xi_max+1,yi_min:yi_max+1] )
            v = np.copy( f1['ts_%06d/v'%ti[tii]][xi_min:xi_max+1,yi_min:yi_max+1] )
            #data['u'][:,:,tiii] = u
            #data['v'][:,:,tiii] = v
            ##
            #data['umag'][:,:,tiii] = np.sqrt(u**2+v**2)
            data[:,:,tiii] = np.sqrt(u**2+v**2)
            ##
            if verbose: progress_bar.update()
        if verbose: progress_bar.close()
    
    if not (data.shape==(nx,ny,nt)):
        raise AssertionError('data.shape!=(nx,ny,nt)')
    
    shape_orig = data.shape
    ndof = nx*ny
    if verbose: even_print('dmd data size', '%0.3f [GB]'%(data.nbytes/1024**3,))
    if verbose: even_print('ndof (nx·ny)', '%i'%(ndof,))
    
    ## reshape data for DMD analysis
    data = np.reshape(data, (ndof,nt), order='C')
    
    ## a dictionary of any 'extra' data in addition to the actual data to return (can get used for plotting etc)
    extra = {'shape_orig':shape_orig,
             'U_inf':U_inf, 'lchar':lchar, 'Re':Re, 'nu_inf':nu_inf,
             'nx':nx, 'ny':ny, 'nt':nt, 'x':x, 'y':y,
             'obstacle':obstacle, 'obstacle_img':obstacle_img }
    
    if verbose: even_print('data shape (original)', '%s'%(str(shape_orig),))
    if verbose: even_print('data shape for dmd', '%s'%(str(data.shape),))
    if verbose: print(72*'-')
    
    return data, extra

class dmd(object):
    '''
    Dynamic Mode Decomposition (DMD)
    
    Description
    -----------
    - Approximate a linear operator 'A' such that x(t+1)=A·x(t) for a snapshot series 'x'
    - For data gathered from a non-linear process, this approximation can be interpreted as a
       linear tangent approximation
    - Inspection of 'A' (eigenvalues, eigenvectors, pseudoeigenvalues, energy amplification, 
          resonance behaviour, etc.) allows for extraction of dynamic characteristics
    
    Bibliography
    ------------
    Schmid, P. J. (2010) Dynamic mode decomposition of numerical and experimental data.
    Journal of Fluid Mechanics 656, doi: 10.1017/S0022112010001217
    
    Jonathan H. Tu, Clarence W. Rowley, Dirk M. Luchtenburg, Steven L. Brunton, J. Nathan Kutz.
    On dynamic mode decomposition: Theory and applications.
    Journal of Computational Dynamics, 2014, 1(2): 391-421. doi: 10.3934/jcd.2014.1.391	shu 
    --> projected vs exact DMD
    
    Jovanovic, M. R., Schmid, P. J. & Nichols, J. W. (2014) Sparsity-promoting dynamic mode decomposition.
    Physics of Fluids 26, doi: 10.1063/1.4863670
    --> http://people.ece.umn.edu/users/mihailo/papers/jovschnicPOF14.pdf
    
    Hemati and Rowley, De-biasing the dynamic mode decomposition
        for applied Koopman spectral analysis, arXiv:1502.03854 (2015).
    --> total least squares DMD
    
    Brunton, S. L., & Kutz, J. N. (2022).
    Data-driven science and engineering: Machine learning, dynamical systems, and control.
    --> http://databookuw.com/databook.pdf
    
    '''
    
    def __init__(self, data, exact=True, optimal=True, svd_rank=None, sort_by_amp_coeff=False):
        '''
        initialize the DMD analyzer
        -----
        - data should be a numpy array with ndim=2
            axis 0 is the data per snapshot
            axis 1 are the snapshots (here interpreted as time, though doesnt have to be)
        - data.shape = (ndof,nt) where e.g. ndof=nx*ny*nz (spatial degrees of freedom)
        '''
        
        print('\n'+'dmd.__init__()')
        print(72*'-')
        
        self.exact = exact
        even_print('exact', '%s'%(exact,))
        self.optimal = optimal
        even_print('optimal', '%s'%(optimal,))
        
        self.svd_rank = svd_rank
        self.sort_by_amp_coeff = sort_by_amp_coeff
        
        if not isinstance(data, np.ndarray):
            raise ValueError('data should be of type np.ndarray')
        if (data.ndim!=2):
            raise ValueError('data should have have ndim=2')
        
        self.data = data
        self.ndof, self.nt = self.data.shape
        
        even_print('data size'  , '%0.3f [GB]'%(self.data.nbytes/1024**3))
        even_print('data shape' , '%s'%str(self.data.shape))
        even_print('data dtype' , '%s'%str(self.data.dtype))
        even_print('n dof'      , '%i'%self.ndof)
        even_print('nt'         , '%i'%self.nt)
        print(72*'-')
    
    # === methods of .run() 
    
    def __get_mean_removed(self,):
        '''
        get mean data (in time), then get mean-removed data
        '''
        
        # if False: ## this is for if the data passed is a numpy stuctured array (outdated)
        #     
        #     ## [t] mean
        #     names   = self.data.dtype.names
        #     formats = [ self.data[a].dtype for a in self.data.dtype.names ]
        #     data_mean = np.zeros(shape=(self.nx,self.ny), dtype={'names':names, 'formats':formats})
        #     for scalar in self.data.dtype.names:
        #         data_mean[scalar][:,:] = np.mean( self.data[scalar], axis=-1 )
        #     
        #     ## get prime (') variables / temporal mean-removed variables
        #     names      = self.data.dtype.names
        #     formats    = [ self.data[a].dtype for a in self.data.dtype.names ]
        #     data_prime = np.zeros(shape=(self.nx,self.ny,self.nt), dtype={'names':names, 'formats':formats})
        #     for scalar in self.data.dtype.names:
        #         data_prime[scalar][:,:,:] = self.data[scalar] - data_mean[scalar][:,:,np.newaxis]
        #     even_print('dmd data size (prime)', '%0.3f [GB]'%(data_prime.nbytes/1024**3,))
        
        #self.data_mean  = np.zeros(shape=(self.ndof,),        dtype=self.data.dtype)
        self.data_prime = np.zeros(shape=(self.ndof,self.nt), dtype=self.data.dtype)
        
        self.data_mean       = np.mean(self.data, axis=-1, dtype=np.float64).astype(self.data.dtype)
        self.data_prime[:,:] = self.data - self.data_mean[:,np.newaxis]
        
        self.snapshot_data = np.copy( self.data_prime )
        
        return
    
    def __do_svd(self,tol=1e-3):
        '''
        singular value decomposition (SVD)
        '''
        
        self.X = self.snapshot_data[:,:-1] ## times [0..nt-1]
        self.Y = self.snapshot_data[:, 1:] ## times [1..nt]
        
        ## compute (reduced) Singular Value Decomposition (SVD) representation of snapshot series X
        ## X = U·Σ·V*
        ##
        start_time = timeit.default_timer()
        U, Sigma, V = np.linalg.svd(self.X, full_matrices=False)
        V = V.conj().T
        ##
        even_print('svd()','%0.2f [s]'%(timeit.default_timer() - start_time))
        even_print('Σ max','%0.8f'%Sigma.max())
        even_print('Σ min','%0.8f'%Sigma.min())
        even_print('log10(Σ_max/Σ_min)','%0.2f'%np.log10(Sigma.max()/Sigma.min()))
        
        # dof,full_rank = U.shape
        # svd_rep_rank = np.linalg.matrix_rank(np.diag(Sigma), tol=tol) #, hermitian=True)
        # even_print('X full rank','%i'%full_rank)
        # even_print('SVD matrix rank (tol=%.1e)'%(tol,),'%i'%svd_rep_rank)
        # even_print('SVD possible reduction', '%i'%(full_rank-svd_rep_rank))
        
        ## make a backup of full SV vector
        self.Sigma_full = np.copy( Sigma )
        even_print('SVD rank (full)', '%i'%(self.Sigma_full.shape[0],))
        
        ## truncate the SVD system
        if (self.svd_rank is None):
            rank = U.shape[1]
        elif (self.svd_rank is not None) and isinstance(self.svd_rank, int):
            rank = min(self.svd_rank, U.shape[1])
        else:
            raise ValueError
        even_print('SVD rank', '%i'%(rank,))
        
        self.U     = np.copy( U[:, :rank]  )
        self.Sigma = np.copy( Sigma[:rank] )
        self.V     = np.copy( V[:, :rank]  )
        
        return
    
    def __get_dmd_modes(self):
        '''
        get DMD modes
        '''
        
        ## https://github.com/mathLab/PyDMD/blob/c98ede0e32e8fb4a690145218f86d39f85da4dc0/pydmd/dmdoperator.py#L51
        
        ## the reduced Koopman operator Ã ('A_tilde')
        ## Ã = (U*)·Y·V·inv(Σ)
        ##
        A_tilde = np.linalg.multi_dot([self.U.T.conj(), self.Y, self.V]) * np.reciprocal(self.Sigma)
        
        ## compute eigenvalues / eigenvectors of Ã
        ## Ã·w = λ·w
        ##
        start_time = timeit.default_timer()
        self.lambda_dmd, self.vr_dmd = sp.linalg.eig(A_tilde, right=True, left=False, overwrite_a=True)
        even_print('get eigvals, eigvecs of Ã','%0.2f [s]'%(timeit.default_timer() - start_time))
        
        if self.exact:
            self.basis = np.copy( np.dot( self.Y, self.V) * np.reciprocal(self.Sigma) ) ## exact
        else:
            self.basis = np.copy( self.U ) ## projected
        
        ## DMD modes
        self.modes = np.dot( self.basis, self.vr_dmd )
        self.dof, self.n_modes = self.modes.shape
        even_print('n modes' , '%i'%(self.n_modes,))
        even_print('n dof' , '%i'%(self.dof,))
        
        #self.modes_L2_norm = sp.linalg.norm(np.abs(self.modes), axis=0, ord=2)
        #print(self.modes_L2_norm)
        
        return
    
    def __get_dmd_amp_coefficients(self):
        '''
        compute amplitude coefficients
        - this can be done 'optimally' (error minimized between modes over all snapshots)
           or this can be done using only a single snapshot
        '''
        
        ## Vandermonde Matrix
        self.vander = np.vander(self.lambda_dmd, self.nt, increasing=True)
        
        ## get amplitude coefficients
        start_time = timeit.default_timer()
        if self.optimal:
            self.alpha_dmd = np.linalg.solve(*self.__get_optimal_fit_matrices())
        else:
            self.alpha_dmd,_,_,_ = sp.linalg.lstsq(self.modes, self.snapshot_data[:,0])
        even_print('get amplitude coeffs','%0.2f [s]'%(timeit.default_timer() - start_time))
        
        ## report data reduction
        original_info_size = self.snapshot_data.nbytes
        dmd_info_size      = self.modes.nbytes + self.alpha_dmd.nbytes
        reduction_info_fac = 1-(dmd_info_size/original_info_size)
        even_print('snapshot data size' , '%0.3f [GB]'%(original_info_size/1024**3,))
        even_print('DMD data size'      , '%0.3f [GB]'%(dmd_info_size/1024**3,))
        even_print('data reduction'     , '%0.2f [%%]'%(reduction_info_fac*100,))
        
        ## sort eigenvalues / eigenvectors / modes by amplitude coefficients
        ## --> not sure if this should even be done, needs verification!
        if self.sort_by_amp_coeff:
            self.alpha_dmd_abs = np.abs(self.alpha_dmd)
            sort_key = np.flip(np.argsort(self.alpha_dmd_abs))
            ##
            self.alpha_dmd  = np.copy( self.alpha_dmd[sort_key]  )
            self.lambda_dmd = np.copy( self.lambda_dmd[sort_key] )
            self.modes      = np.copy( self.modes[:,sort_key]    )
            self.vander     = np.copy( self.vander[sort_key,:]   )
        
        return
    
    def __get_optimal_fit_matrices(self,):
        '''
        get P,q for optimal amplitude coefficient calculation
        '''
        P = np.multiply( np.dot(self.modes.conj().T, self.modes),
                         np.conj(np.dot(self.vander, self.vander.conj().T))
                       )
        
        if self.exact:
            q = np.conj(np.diag(np.linalg.multi_dot([self.vander,
                                                     self.snapshot_data.conj().T,
                                                     self.modes])))
        else:
            q = np.conj(np.diag(
                np.linalg.multi_dot([self.vander[:, :-1],
                                     self.V,
                                     #self.V.conj().T,
                                     np.diag(self.Sigma).conj(),
                                     self.vr_dmd])))
        return P,q
    
    def run(self,):
        '''
        run wrapper to fit data (do SVD, get modes, assimilate data to modes)
        '''
        print('\n'+'dmd.run()')
        print(72*'-')
        self.__get_mean_removed()
        self.__do_svd()
        self.__get_dmd_modes()
        self.__get_dmd_amp_coefficients()
        #self.reconstruct(r=None)
        print(72*'-')
        return
    
    # ===
    
    def reconstruct(self,r=None):
        '''
        reconstruct the snapshot data from DMD modes
        '''
        print('\n'+'dmd.reconstruct()')
        print(72*'-')
        
        if (r is None):
            r = self.n_modes
        even_print('reconstruction rank','%i'%(r,))
        
        start_time = timeit.default_timer()
        self.snapshot_data_reconst = np.real( np.dot(self.modes[:,:r], np.dot(np.diag(self.alpha_dmd[:r]), self.vander[:r,:])) )
        even_print('do reconstruction','%0.2f [s]'%(timeit.default_timer() - start_time))
        ##
        self.snapshot_data_delta = self.snapshot_data_reconst - self.snapshot_data
        adiff = np.mean( np.abs( self.snapshot_data_delta ) )
        even_print('reconstruction: mean absolute diff','%0.3e'%(adiff,))
        print(72*'-')
        
        return

# main
# ======================================================================

if __name__ == '__main__':
    
    if True: ## retrieve data, run DMD, save
        
        # === identify data file for DMD & load data into memory
        fn_h5 = Path('../data/lbm_cylinder_1/data.h5')
        if not fn_h5.exists():
            raise FileNotFoundError('%s not found!'%fn_h5)
        data, extra = dmd_data_loader_lbm(fn_h5,
                                          scalar='umag',
                                          ti_min=6001-900,
                                          ti_max=6001 )
        
        # === run DMD
        d = dmd(data=data,
                svd_rank=100,
                exact=False,
                optimal=True,
                sort_by_amp_coeff=True
                )
        d.run()
        
        # === serialize data to binary file
        with open('dmd.dat','wb') as f:
            pickle.dump([d,extra], f, protocol=4)
    
    else: ## load from pickled file
        
        with open('dmd.dat','rb') as f:
            d,extra = pickle.load(f)
    
    ## reconstruct umag' field
    d.reconstruct(r=None)
    
    # ============================== #
    # plot
    # ============================== #
    
    ## set up plotting environment
    mpl.style.use('dark_background')
    colors = ['#ff44ff', '#00d7ff', '#00ffff', '#00fb00', '#f9d300', '#ff8800', '#ff004d'] ## dark bg
    #colors = ['#d339ee', '#0085ff', '#00a6a0', '#009b00', '#9c8300', '#e15800', '#ff0033'] ## white bg
    purple, blue, cyan, green, yellow, orange, red = colors
    cl1 = blue; cl2 = yellow; cl3 = red; cl4 = green; cl5 = purple; cl6 = orange; cl7 = cyan
    
    ## get data from 'extra' dictionary from the dmd_data_loader
    nx = extra['nx']
    ny = extra['ny']
    nt = extra['nt']
    U_inf = extra['U_inf']
    lchar = extra['lchar']
    x = extra['x']
    y = extra['y']
    obstacle_img = extra['obstacle_img']
    
    ## for imshow()
    extent=[ (x/lchar).min(), (x/lchar).max(), (y/lchar).min(), (y/lchar).max() ]
    
    if True: ## plot: singular values Σ
        
        fn_png1 = 'sigma.png'
        fn_png2 = 'sigma_log.png'
        
        plt.close('all')
        fig1 = plt.figure(figsize=(6,6/(32/15)), dpi=200)
        ax1 = plt.gca()
        #ax1.set_aspect('equal')
        ax1.tick_params(axis='x', which='both', direction='out')
        ax1.tick_params(axis='y', which='both', direction='out')
        #ax1.set_yscale('log',base=10)
        ##
        if hasattr(d,'Sigma_full'):
            diff = d.Sigma_full.shape[0] - d.Sigma.shape[0]
            if (diff>0):
                aa = np.arange(d.Sigma.shape[0],d.Sigma.shape[0]+diff)
                bb = d.Sigma_full[-diff:]
                ax1.scatter(aa, \
                            bb, \
                            c='gray', zorder=2, \
                            s=2, marker='o', linewidth=0, edgecolors='k', alpha=1.0)
        ##
        ax1.scatter(np.arange(d.Sigma.shape[0]), \
                    d.Sigma, \
                    c=red, zorder=3, \
                    s=2, marker='o', linewidth=0, edgecolors='k', alpha=1.0)
        ##
        ax1.set_xlabel(r'$i$')
        ax1.set_ylabel(r'$\Sigma_{i}$')
        ##
        ax1.set_xlim(left=-1)
        #ax1.set_ylim(bottom=0)
        ##
        fig1.tight_layout(pad=0.25)
        fig1.tight_layout(pad=0.25)
        ##
        dpi_out = 2160/plt.gcf().get_size_inches()[1]
        fig1.savefig(fn_png1, dpi=dpi_out)
        #plt.show()
        ##
        ## save a semilog version of same plot
        ax1.set_yscale('log',base=10)
        fig1.tight_layout(pad=0.25)
        fig1.tight_layout(pad=0.25)
        fig1.savefig(fn_png2, dpi=dpi_out)
        ##
        #plt.show()
        pass
    
    if True: ## plot: alpha_dmd (amplitude coefficients)
        
        fn_png = 'alpha.png'
        
        r = 9999999
        r = min(r,d.alpha_dmd.shape[0])
        
        plt.close('all')
        fig1 = plt.figure(figsize=(6*1.2,6), dpi=120)
        ax1 = plt.gca()
        ax1.set_aspect('equal')
        ax1.tick_params(axis='x', which='both', direction='out')
        ax1.tick_params(axis='y', which='both', direction='out')
        
        ax1.axvline(x=0., linestyle='dashed', linewidth=0.5, c='lightgray', alpha=1.0, zorder=2, label=None)
        ax1.axhline(y=0., linestyle='dashed', linewidth=0.5, c='lightgray', alpha=1.0, zorder=2, label=None)
        ##
        ## plot eigenvalues on complex plane
        ax1.scatter(np.real(d.alpha_dmd[:r]),
                    np.imag(d.alpha_dmd[:r]),
                    c=blue,
                    zorder=3,
                    s=5, marker='o', linewidth=0, edgecolors='k', alpha=1.0)
        ##
        ## annotate the scatterplot
        for ri in range(r):
            ax1.annotate(('%i'%ri),
                         xy=(np.real(d.alpha_dmd[ri]), np.imag(d.alpha_dmd[ri])),
                         xycoords='data',
                         xytext=(2,2), 
                         va='center',
                         ha='left',
                         color=ax1.xaxis.label.get_color(),
                         zorder=4,
                         textcoords='offset points',
                         fontsize=6)
        ##
        rng_x = max(np.abs(np.real(d.alpha_dmd)))
        rng_y = max(np.abs(np.imag(d.alpha_dmd)))
        rng   = max(rng_x,rng_y)
        ##
        ax1.set_xlim(-1.2*rng, 1.2*rng)
        ax1.set_ylim(-1.2*rng, 1.2*rng)
        ##
        ax1.set_xlabel(r'Re($\alpha_{i}$)')
        ax1.set_ylabel(r'Im($\alpha_{i}$)')
        ##
        fig1.tight_layout(pad=0.25)
        fig1.tight_layout(pad=0.25)
        ##
        dpi_out = 2160/plt.gcf().get_size_inches()[1]
        fig_trim_x(fig1, [ax1,], offset_px=16, dpi=dpi_out)
        fig1.savefig(fn_png, dpi=dpi_out)
        ##
        #plt.show()
        pass
    
    if True: ## plot: DMD eigenvalues on unit circle
        
        fn_png = 'lambda.png'
        
        plt.close('all')
        fig1 = plt.figure(figsize=(6*1.2,6), dpi=120)
        ax1 = plt.gca()
        ax1.set_aspect('equal')
        ax1.tick_params(axis='x', which='both', direction='out')
        ax1.tick_params(axis='y', which='both', direction='out')
        
        ## circle (outer)
        c1 = plt.Circle( [0,0],
                         radius=1.,
                         color=ax1.xaxis.label.get_color(),
                         alpha=1.0,
                         fill=False,
                         ls='dashed',
                         lw = 0.5,
                         zorder=0
                         )
        ax1.add_patch(c1)
        
        ## plot eigenvalues on complex plane
        ax1.scatter(np.real(d.lambda_dmd),
                    np.imag(d.lambda_dmd),
                    c=yellow,
                    zorder=3,
                    s=4, marker='o', linewidth=0, edgecolors='k', alpha=1.0)
        
        ax1.set_xlabel(r'Re($\lambda_{i}$)')
        ax1.set_ylabel(r'Im($\lambda_{i}$)')
        ##
        fig1.tight_layout(pad=0.25)
        fig1.tight_layout(pad=0.25)
        ##
        dpi_out = 2160/plt.gcf().get_size_inches()[1]
        fig_trim_x(fig1, [ax1,], offset_px=16, dpi=dpi_out)
        fig1.savefig(fn_png, dpi=dpi_out)
        ##
        #plt.show()
        pass
    
    if True: ## plot: mean(umag)
        
        data_mean = np.reshape( np.copy(d.data_mean), (nx,ny), order='C' )
        
        norm = mpl.colors.Normalize(vmin=0, vmax=2)
        #cmap = mpl.cm.RdBu_r
        cmap = cmasher.cm.iceburn
        
        fn_png = 'data_mean.png'
        
        plt.close('all')
        fig1 = plt.figure(figsize=(8,8/2), dpi=200)
        ax1 = plt.gca()
        ax1.set_aspect('equal')
        ax1.tick_params(axis='x', which='both', direction='out')
        ax1.tick_params(axis='y', which='both', direction='out')
        ##
        im1 = ax1.imshow(data_mean.T/U_inf,
                         origin='lower', aspect='equal',
                         interpolation='none',
                         extent=extent,
                         cmap=cmap, norm=norm)
        ##
        im1_obstacle = ax1.imshow(obstacle_img.transpose(1,0,2),
                                  origin='lower', aspect='equal',
                                  extent=extent,
                                  interpolation='none')
        ##
        ax1.xaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
        ax1.xaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
        ax1.yaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
        ax1.yaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
        ##
        cbar = fig1.colorbar(im1, ax=ax1, orientation='vertical', aspect=30, fraction=0.1, pad=0.005)
        cbar.ax.set_ylabel(r'$\overline{u_{mag}}/U_{\infty}$') # rotation=90, labelpad=10
        cbar.ax.tick_params(axis='y', which='both', direction='out')
        #cbar.set_ticks([2.80,2.85,2.90,2.95,3.00])
        ##
        ax1.set_xlabel(r'$x/\ell_{char}$')
        ax1.set_ylabel(r'$y/\ell_{char}$')
        ##
        fig1.tight_layout(pad=0.25)
        fig1.tight_layout(pad=0.25)
        ##
        x0A, y0A, dxA, dyA = ax1.get_position().bounds
        x0B, y0B, dxB, dyB = cbar.ax.get_position().bounds
        cbar.ax.set_position([x0B, y0A, dxB, dyA])
        ##
        dpi_out = 2160/plt.gcf().get_size_inches()[1]
        fig_trim_x(fig1, [ax1,cbar.ax], offset_px=16, dpi=dpi_out)
        fig1.savefig(fn_png, dpi=dpi_out)
        ##
        #plt.show()
        pass
    
    if True: ## plot: umag' at t=0
        
        umagI = np.reshape( d.snapshot_data, (nx,ny,nt), order='C')[:,:,0]
        
        norm = mpl.colors.Normalize(vmin=-1, vmax=+1)
        #cmap = mpl.cm.RdBu_r
        cmap = cmasher.cm.iceburn
        
        fn_png = 'umagI.png'
        
        plt.close('all')
        fig1 = plt.figure(figsize=(8,8/2), dpi=200)
        ax1 = plt.gca()
        ax1.set_aspect('equal')
        ax1.tick_params(axis='x', which='both', direction='out')
        ax1.tick_params(axis='y', which='both', direction='out')
        ##
        im1 = ax1.imshow(umagI.T/U_inf,
                         origin='lower', aspect='equal',
                         extent=extent,
                         interpolation='none', 
                         cmap=cmap, norm=norm)
        ##
        im1_obstacle = ax1.imshow(obstacle_img.transpose(1,0,2),
                                  origin='lower', aspect='equal',
                                  extent=extent,
                                  interpolation='none')
        ##
        ax1.xaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
        ax1.xaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
        ax1.yaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
        ax1.yaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
        ##
        cbar = fig1.colorbar(im1, ax=ax1, orientation='vertical', aspect=30, fraction=0.1, pad=0.005)
        cbar.ax.set_ylabel(r'$u_{mag}^{\prime}/U_{\infty}$') # rotation=90, labelpad=10
        cbar.ax.tick_params(axis='y', which='both', direction='out')
        #cbar.set_ticks([2.80,2.85,2.90,2.95,3.00])
        ##
        ax1.set_xlabel(r'$x/\ell_{char}$')
        ax1.set_ylabel(r'$y/\ell_{char}$')
        ##
        fig1.tight_layout(pad=0.25)
        fig1.tight_layout(pad=0.25)
        ##
        x0A, y0A, dxA, dyA = ax1.get_position().bounds
        x0B, y0B, dxB, dyB = cbar.ax.get_position().bounds
        cbar.ax.set_position([x0B, y0A, dxB, dyA])
        ##
        dpi_out = 2160/plt.gcf().get_size_inches()[1]
        fig_trim_x(fig1, [ax1,cbar.ax], offset_px=16, dpi=dpi_out)
        fig1.savefig(fn_png, dpi=dpi_out)
        ##
        #plt.show()
        pass
    
    if True: ## plot: umag' (reconstructed)
        
        umagI_reconst = np.reshape( d.snapshot_data_reconst.real, (nx,ny,nt), order='C')[:,:,0]
        
        norm = mpl.colors.Normalize(vmin=-1, vmax=+1)
        #cmap = mpl.cm.RdBu_r
        cmap = cmasher.cm.iceburn
        
        fn_png = 'umagI_reconst.png'
        
        plt.close('all')
        fig1 = plt.figure(figsize=(8,8/2), dpi=200)
        ax1 = plt.gca()
        ax1.set_aspect('equal')
        ax1.tick_params(axis='x', which='both', direction='out')
        ax1.tick_params(axis='y', which='both', direction='out')
        ##
        im1 = ax1.imshow(umagI_reconst.T/U_inf,
                         origin='lower', aspect='equal',
                         interpolation='none',
                         extent=extent,
                         cmap=cmap, norm=norm)
        ##
        im1_obstacle = ax1.imshow(obstacle_img.transpose(1,0,2),
                                  origin='lower', aspect='equal',
                                  extent=extent,
                                  interpolation='none')
        ##
        ax1.xaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
        ax1.xaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
        ax1.yaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
        ax1.yaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
        ##
        cbar = fig1.colorbar(im1, ax=ax1, orientation='vertical', aspect=30, fraction=0.1, pad=0.005)
        cbar.ax.set_ylabel(r'$u_{mag}^{\prime}/U_{\infty}$'+' (reconstructed)') # rotation=90, labelpad=10
        cbar.ax.tick_params(axis='y', which='both', direction='out')
        #cbar.set_ticks([2.80,2.85,2.90,2.95,3.00])
        ##
        ax1.set_xlabel(r'$x/\ell_{char}$')
        ax1.set_ylabel(r'$y/\ell_{char}$')
        ##
        fig1.tight_layout(pad=0.25)
        fig1.tight_layout(pad=0.25)
        ##
        x0A, y0A, dxA, dyA = ax1.get_position().bounds
        x0B, y0B, dxB, dyB = cbar.ax.get_position().bounds
        cbar.ax.set_position([x0B, y0A, dxB, dyA])
        ##
        dpi_out = 2160/plt.gcf().get_size_inches()[1]
        fig_trim_x(fig1, [ax1,cbar.ax], offset_px=16, dpi=dpi_out)
        fig1.savefig(fn_png, dpi=dpi_out)
        ##
        #plt.show()
        pass
    
    if True: ## plot: 1x DMD mode
        
        i_mode = 0
        fn_png = 'mode_%04d.png'%(i_mode,)
        
        modeX = np.reshape( d.modes[:,i_mode].real, (nx,ny), order='C')
        
        norm = mpl.colors.Normalize(vmin=-np.abs(modeX).max(), vmax=+np.abs(modeX).max())
        #cmap = mpl.cm.RdBu_r
        cmap = cmasher.cm.redshift
        ##
        plt.close('all')
        fig1 = plt.figure(figsize=(8,8/2), dpi=200)
        ax1 = plt.gca()
        ax1.set_aspect('equal')
        ax1.tick_params(axis='x', which='both', direction='out')
        ax1.tick_params(axis='y', which='both', direction='out')
        ##
        im1 = ax1.imshow(modeX.T, origin='lower',
                         aspect='equal',
                         extent=extent,
                         interpolation='none', 
                         cmap=cmap, norm=norm )
        ##
        im1_obstacle = ax1.imshow(obstacle_img.transpose(1,0,2),
                                  origin='lower', aspect='equal',
                                  extent=extent,
                                  interpolation='none')
        ##
        ax1.xaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
        ax1.xaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
        ax1.yaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
        ax1.yaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
        ##
        ax1.set_xlabel(r'$x/\ell_{char}$')
        ax1.set_ylabel(r'$y/\ell_{char}$')
        ##
        fig1.tight_layout(pad=0.25)
        fig1.tight_layout(pad=0.25)
        ##
        dpi_out = 2160/plt.gcf().get_size_inches()[1]
        fig_trim_x(fig1, [ax1,], offset_px=16, dpi=dpi_out)
        fig1.savefig(fn_png, dpi=dpi_out)
        ##
        #plt.show()
        pass
    
    if True: ## plot: all DMD modes
        
        if not os.path.isdir('modes'):
            Path('modes').mkdir(parents=True, exist_ok=True)
        
        for i_mode in range(d.modes.shape[1]):
            
            fn_png = 'modes/mode_%04d.png'%(i_mode,)
            
            modeX = np.reshape( d.modes[:,i_mode].real, (nx,ny), order='C')
            
            norm = mpl.colors.Normalize(vmin=-np.abs(modeX).max(), vmax=+np.abs(modeX).max())
            #cmap = mpl.cm.RdBu_r
            cmap = cmasher.cm.redshift
            ##
            plt.close('all')
            fig1 = plt.figure(figsize=(8,8/2), dpi=100)
            ax1 = plt.gca()
            ax1.set_aspect('equal')
            ax1.tick_params(axis='x', which='both', direction='out')
            ax1.tick_params(axis='y', which='both', direction='out')
            ##
            im1 = ax1.imshow(modeX.T,
                             origin='lower', aspect='equal',
                             extent=extent,
                             interpolation='none', 
                             cmap=cmap, norm=norm )
            ##
            im1_obstacle = ax1.imshow(obstacle_img.transpose(1,0,2),
                                      origin='lower', aspect='equal',
                                      extent=extent,
                                      interpolation='none')
            ##
            ax1.xaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
            ax1.xaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
            ax1.yaxis.set_major_locator(mpl.ticker.MultipleLocator(1))
            ax1.yaxis.set_minor_locator(mpl.ticker.MultipleLocator(0.2))
            ##
            ax1.set_xlabel(r'$x/\ell_{char}$')
            ax1.set_ylabel(r'$y/\ell_{char}$')
            ##
            fig1.tight_layout(pad=0.25)
            fig1.tight_layout(pad=0.25)
            ##
            dpi_out = 2160/plt.gcf().get_size_inches()[1]
            fig_trim_x(fig1, [ax1,], offset_px=16, dpi=dpi_out)
            fig1.savefig(fn_png, dpi=dpi_out)
            #plt.show()
            pass
    