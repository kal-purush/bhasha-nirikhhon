#!/usr/bin/env python3.6

version='3.0.0'
name='optimized_graphic_presets'

gamePackageName=f'zzz_{name}.pak'
zipFilename=f'kcd_{name}_{version}.zip'

import os
import zipfile


zout = zipfile.ZipFile( gamePackageName, 'w', compression=zipfile.ZIP_STORED )
directory = os.path.join('Config', 'CVarGroups')
for file in os.listdir(directory):
	filename = os.fsdecode(file)
	zout.write( os.path.join(directory, filename) )
zout.close()


zout = zipfile.ZipFile( zipFilename, 'w', compression=zipfile.ZIP_DEFLATED )
directory = 'Config'
for file in os.listdir(directory):
	filename = os.fsdecode(file)
	if os.path.isfile(os.path.join(directory, filename) ):
		zout.write( os.path.join(directory, filename) )
directory = 'optimized_graphic_presets'
for file in os.listdir(directory):
	filename = os.fsdecode(file)
	zout.write( os.path.join(directory, filename) )
zout.write( gamePackageName, arcname=os.path.join('Data', gamePackageName) )
zout.close()