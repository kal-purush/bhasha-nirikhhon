'''Script to create BPM based in preBPM, using the directions from Robert.
'''

import os
import sys
import subprocess
import time
import numpy as np
import shlex

class Toolbox():
    @classmethod
    def progress_bar(cls,iterator,Nposit,wait_time=0.25):
        '''Receives the actual iterator and the max number of items 
        Idea from: http://stackoverflow.com/questions/3002085/
        python-to-print-out-status-bar-and-percentage
        '''
        sys.stdout.write('\r')
        aux = (iterator*100/Nposit)-1
        sys.stdout.write('|{0:{1}}| {2}%'.format('='*iterator,Nposit,aux))
        sys.stdout.flush()
        time.sleep(wait_time)


class Listed():
    def __init__(self,fn_pixcor,fn_precal,compression=False):
        '''Load the two inputs tables, assuming no compression in the 
        pixcor (fn_pixcor) files
        '''
        #IMPROVEMENT: accept user-defined dtypes and tables with
        #other elements, to use only selected columns
        aux_dt1 = [('expnum','i4'),('ccdnum','i4'),('band','|S10'),
                ('root','|S100'),('path','|S100'),('filename','|S50')]
        if compression:
            aux_dt1.append(('compression','|S10'))
        dt1 = np.dtype(aux_dt1)
        dt2 = np.dtype([('archive_path','|S100'),('reqnum','i4'),
                    ('unitname','i4'),('attnum','i4')])
        A = np.loadtxt(fn_pixcor,dtype=dt1,comments='#',skiprows=0,usecols=None)
        B = np.loadtxt(fn_precal,dtype=dt2,comments='#',skiprows=0,usecols=None)
        self.pieces = A
        self.precal = B

    @classmethod
    def feed_list(cls,fn_pixcor,fn_precal,compression,ccdnum,BPM_band):
        '''Create lists to be used as inputs for the BPM creation code
        '''
        prec = Listed(fn_pixcor,fn_precal,compression).precal
        biascor = []
        flatcor = []
        for n in xrange(prec.shape[0]):
            for iter_ccd in ccdnum:
                fn_bias = 'D_n{0}_c{1:02}_r{2}p{3:02}_biascor.fits'.format(
                    prec['unitname'][n],iter_ccd,prec['reqnum'][n],
                    prec['attnum'][n])
                aux_line = os.path.join('/archive_data/desarchive',
                                    *[prec['archive_path'][n],
                                    'biascor',fn_bias])
                biascor.append((aux_line,iter_ccd))
                for bb in BPM_band:
                    fn_flat = 'D_n{0:08}_{1}_c{2:02}_r{3}p{4:02}'.format(
                        prec['unitname'][n],bb,iter_ccd,
                        prec['reqnum'][n],prec['attnum'][n])
                    fn_flat += '_norm-dflatcor.fits'
                    tmp_line = os.path.join('/archive_data/desarchive',
                                        *[prec['archive_path'][n],
                                        'norm-dflatcor',fn_flat])
                    flatcor.append((tmp_line,iter_ccd))
        #CHECK THIS: its ok to only use g-band on flatcor list?
        ccdN = Listed(fn_pixcor,fn_precal,compression).pieces
        construct = lambda x,y,z,ccd: (os.path.join(x,*[y,z]),ccd)
        obj = [construct(x['root'],x['path'],x['filename'],x['ccdnum']) 
            for x in ccdN]
        dt = np.dtype([('path','|S200'),('ccdnum','i4')])
        biascor = np.array(biascor,dtype=dt)
        flatcor = np.array(flatcor,dtype=dt)
        obj = np.array(obj,dtype=dt) 
        return biascor,flatcor,obj
    
    @classmethod
    def make_bpm(cls,fn_pixcor,fn_precal,compression=False,
                ccdnum=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
                21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
                41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,
                62],
                BPM_band=['g'],
                badpix_dir='/work/devel/fpazch/desdmSVN_copy/devel/despycal/trunk/data',
                exe_dir='/work/devel/fpazch/desdmSVN_copy/devel/despycal/trunk/bin',
                label='test',
                clean_tmp=True):
        '''Method to run the code and fill the logs, using the PID as 
        auxiliary for filename/path creation
        '''
        PID = os.getpid()
        biascor,flatcor,obj = Listed.feed_list(fn_pixcor,fn_precal,
                                            compression,ccdnum,BPM_band)
        #create output folder
        dir_out = 'out_{0}_pid{1}'.format(label,PID)
        dir_log = 'log_{0}_pid{1}'.format(label,PID)
        mkout = subprocess.check_call(['mkdir',dir_out])
        mklog = subprocess.check_call(['mkdir',dir_log])

        #BPM call, per CCD
        print '\tBPM per CCD\n\t%s'%('^'*11)
        for m in ccdnum:
            Toolbox.progress_bar(m,len(ccdnum),wait_time=0.5)
            #write out tmp files
            out_bias = 'pid{0}.biascor.csv'.format(PID)
            np.savetxt(out_bias,biascor[biascor['ccdnum']==m]['path'],fmt='%s')
            out_flat = 'pid{0}.flatcor.csv'.format(PID)
            np.savetxt(out_flat,flatcor[flatcor['ccdnum']==m]['path'],fmt='%s')
            out_obj = 'pid{0}.objects.csv'.format(PID)
            np.savetxt(out_obj,obj[obj['ccdnum']==m]['path'],fmt='%s')

            badpix = os.path.join(badpix_dir,'bad_pixel_20160506.lst')
            funky = os.path.join(badpix_dir,'funky_column.lst')
            
            cmds = os.path.join(exe_dir,'mkbpm.py')
            cmds += ' --outfile {0}/bpm_c{1:02}.fits'.format(dir_out,m)
            cmds += ' --ccdnum {0}'.format(m)
            cmds += ' --biascor {0}'.format(out_bias)
            cmds += ' --flatcor {0}'.format(out_flat)
            cmds += ' --images {0}'.format(out_obj)
            cmds += ' --badpix {0}'.format(badpix)
            cmds += ' --funkycol {0}'.format(funky)
            cmds += ' --verbose 3'

            #shlex to more refinated way to take care of tokenization
            cmds = shlex.split(cmds)
            #Notes:
            #i) remember to *maybe* use stdin for communicate different args
            #to mkbpm
            logbpm = open('{0}/log.bpm_c{1:02}'.format(dir_log,m),'w+')
            job = subprocess.Popen(cmds,stdin=None,
                                stdout=logbpm,
                                stderr=subprocess.STDOUT,
                                universal_newlines=True,
                                shell=False,bufsize=-1)
            job.wait()
            logbpm.close()
            
            #erase the tmp files
            if clean_tmp:
                rm_bias = subprocess.check_call(['rm',out_bias]) 
                rm_flat = subprocess.check_call(['rm',out_flat]) 
                rm_obj = subprocess.check_call(['rm',out_obj]) 
        print '\n\tOutput directory: {0}\n\tLOG directory: {1}'.format(dir_out,
                                                                    dir_log)

    @classmethod
    def fill_log(cls):
        pass


if __name__=='__main__':
    '''use_ccd = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
            21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
            41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,
            62]
    '''
    Listed.make_bpm('y4e2_object.list','y4e2_precal.list',label='y4e2')