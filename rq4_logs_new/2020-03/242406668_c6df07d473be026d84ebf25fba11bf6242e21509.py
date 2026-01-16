from photopipe.reduction import preproc
from photopipe.reduction.auto.autoproc import autoproc
from shutil import move
import os
import glob
from zipfile import ZipFile

print("creating paths")
base_path = os.path.dirname(os.path.abspath(__file__))
print('base_path', base_path)
test_path = os.path.join(base_path, 'test')
print('test_path', test_path)
copy_path = os.path.join(test_path, 'copy')
print('copy_path', copy_path)
selected_path = os.path.join(copy_path, 'selected')
print('selected_path', selected_path)
reduced_path = os.path.join(copy_path, 'reduced')
print('reduced_path', reduced_path)
zip_file = os.path.join(test_path, 'test.zip')
print(zip_file, zip_file)
zf = ZipFile(zip_file, 'r')
# print('extracting files')
zf.extractall(copy_path)
print('extraction complete')

print('start bias calibration')
bias_calib = preproc.choose_calib(
    'lmi',
    'bias',
    workdir=copy_path+os.path.sep,
    cams=[0],
    auto=True,
    amin=0.0, amax=1.0,
    reject_sat=False,
    save_select=True,
    noplot=True
)

print('start flat calibration')
flat_calib = preproc.choose_calib(
    'lmi',
    'flat',
    workdir=copy_path+os.path.sep,
    cams=[0],
    auto=True,
    amin=0.2, amax=0.8,
    reject_sat=False,
    save_select=True,
    noplot=True
)

print('start choose science')
science_dict = preproc.choose_science(
    'lmi',
    workdir=copy_path+os.path.sep,
    targetdir=selected_path+os.path.sep,
    cams=[0],
    auto=True,
    save_select=True,
    calibrate=False,
    noplot=True
)

print('start mkmaster bias')
preproc.mkmaster('lmi', bias_calib, 'bias')
print('start mkmaster flat')
preproc.mkmaster('lmi', flat_calib, 'flat')

print('start move files master biases to selected folder')
for f in glob.glob(os.path.join(base_path, 'bias*.fits')):
    filename = os.path.basename(f)
    print('selected_path', selected_path)
    f_base_path = os.path.join(base_path, filename)
    print('f_base_path', f_base_path)
    f_selected_path = os.path.join(selected_path, filename)
    print('f_selected_path', f_selected_path)
    print('moving {0} to {1}'.format(f_base_path, f_selected_path))
    move(f_base_path, f_selected_path)

print('start move files master flats to selected folder')
for f in glob.glob(os.path.join(base_path, 'flat*.fits')):
    filename = os.path.basename(f)
    print('selected_path', selected_path)
    f_base_path = os.path.join(base_path, filename)
    print('f_base_path', f_base_path)
    f_selected_path = os.path.join(selected_path, filename)
    print('f_selected_path', f_selected_path)
    print('moving {0} to {1}'.format(f_base_path, f_selected_path))
    move(f_base_path, f_selected_path)

autoproc(datadir=selected_path+os.path.sep,
         imdir=reduced_path+os.path.sep,
         redo=1, nomastersky=True)