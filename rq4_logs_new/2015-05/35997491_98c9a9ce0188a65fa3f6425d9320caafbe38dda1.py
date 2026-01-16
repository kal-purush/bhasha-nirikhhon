from flask import Flask, render_template, request, url_for, send_from_directory, jsonify, send_file
from flask_jsglue import JSGlue
from psf_form import PSFForm
import psf
from PIL import Image
import numpy as np
from math import asin,sin,cos
from scipy.ndimage.interpolation import zoom
from StringIO import StringIO
app = Flask(__name__)
jsglue = JSGlue(app)

UPLOAD_FOLDER = 'uploads/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def samples(wavelen,num_aperture,refr_index,mf=1):
    alpha = asin(num_aperture/refr_index)
    nyx = wavelen/(4*mf)/refr_index/sin(alpha)
    nyz = wavelen/(2*mf)/refr_index/(1 - cos(alpha))
    return (nyx,nyz)
    
def calculate_nyquist(microscope,ex_wavelen,em_wavelen,num_aperture,refr_index):
    
    if ('Confocal' in microscope) or ('Two photon' in microscope):
        return samples(ex_wavelen,num_aperture,refr_index,mf=2)
    if 'Widefield' in microscope:
        return samples(em_wavelen,num_aperture,refr_index)
        
def save_psf(data):
    arr = psf.mirror_symmetry(np.log10(data))
    arr[np.isneginf(arr)] = 0.0
    arr[np.where(arr < -2.5)] = 0.0
    arr = arr + abs(np.amin(arr))
    disp_min = np.amin(arr)
    disp_max = np.amax(arr)
    newarr = (arr * (255 / (disp_max - disp_min))).astype(np.uint8)
    newarr[np.where(newarr == 255)] = 0
    newarr[31,31] = 255
    im = Image.fromarray(newarr)
    im = im.convert("RGB")
    imgpath = UPLOAD_FOLDER+'psf.png'
    im.save(imgpath)
    return imgpath
        
def calculate_psf(model,scope,ex_wavelen,em_wavelen,num_aperture,refr_index):
    args = dict(shape=(32,32), dims=(4,4), ex_wavelen=ex_wavelen, em_wavelen=em_wavelen,
             num_aperture=num_aperture, refr_index=refr_index,
             pinhole_radius=0.50, pinhole_shape='round')
    return psf.PSF(model | scope, **args)  
    
@app.route('/update_psf/<filename>',methods=['GET'])      
def update_psf(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'],
                               filename)
    
@app.route('/get_psf',methods=['GET','POST'])
def get_psf():
    form = PSFForm()
    if (request.method == 'POST'):
        model = form.model.data
        print 'model',model
        if 'Gaussian' in model:
            calc_model = psf.GAUSSIAN
        if 'Isotropic' in model:
            calc_model = psf.ISOTROPIC
        
        microscope = form.microscope.data
        print 'microscope',microscope
        if 'Confocal' in microscope:
            scope = psf.CONFOCAL
        if 'Widefield' in microscope:
            scope = psf.WIDEFIELD
        if 'Two photon' in microscope:
            scope = psf.TWOPHOTON
        
        ex_wavelen = float(form.ex_wavelen.data)
        em_wavelen = float(form.em_wavelen.data)
        num_aperture = float(form.num_aperture.data)
        refr_index = float(form.refr_index.data)
    
        obsvol = calculate_psf(calc_model,scope,ex_wavelen,em_wavelen,num_aperture,refr_index)
        imgpath = save_psf(obsvol.data)
        
        if 'Gaussian' in model:
            sigma = [sig*1000*2.355 for sig in obsvol.sigma.um]
        else:
            sigma = ['N/A','N/A']
        nyquist = calculate_nyquist(microscope,ex_wavelen,em_wavelen,num_aperture,refr_index)
        data = {"sigmar": str(sigma[0]),
                "sigmaz": str(sigma[1]),
                "microscope": microscope,
                "model": model,
                "nyquistxy": str(nyquist[0]),
                "nyquistz": str(nyquist[1]),
                "url": imgpath}
        return jsonify(data)
    elif request.method == 'GET':
        sigma = None
        microscope = None
        nyquist = None
        image = None
    return render_template('psf.html',form=form,sigma=sigma,\
                                      microscope=microscope,\
                                      nyquist=nyquist,
                                      image=image)

if __name__ == '__main__':
    app.debug = True
    app.secret_key = 's3cr3t'
    app.run()