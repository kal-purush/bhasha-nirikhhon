import subprocess
import shlex
import os
import sys
import shutil

parentPath = '/'.join( sys.path[0].replace('\\', '/').split('/')[:-1] )
sys.path.append( parentPath )

import Forge
import Coal


###############################################################################################
# Version
###############################################################################################


coalMilestoneVersion = 0 # for announcing major milestones - may contain all of the below
coalMajorVersion     = 0 # backwards-incompatible changes
coalMinorVersion     = 0 # new backwards-compatible features
coalPatchVersion     = 1 # bug fixes


###############################################################################################
# Environment
###############################################################################################


softwareEnvironment = 'f:/software/'
softwareName = 'coal_%s.%s.%s.%sdev/%s' %( coalMilestoneVersion, coalMajorVersion, coalMinorVersion, coalPatchVersion, 'Coal' )
softwarePath = '%s%s/' %( softwareEnvironment, softwareName )


###############################################################################################
# Folder creation
###############################################################################################


curentPath = os.path.dirname(os.path.realpath(__file__))
binDir  = 'bin'
coreDir = 'core'
uiDir   = 'ui'

Fsystem = Forge.core.System()

for folder in [ coreDir, uiDir, binDir ]:
	Fsystem.mkdir( '%s%s' %(softwarePath, folder) )

###############################################################################################
# Moving compiles files
###############################################################################################


print '>>> Install Begin'

for folder in [ curentPath, coreDir, uiDir, binDir ]:
	for file in os.listdir( folder ):
		currentFile = '%s/%s' %( folder, file )
		newFile = '%s%s/%s' %( softwarePath, folder, file )

		if folder == binDir:
			shutil.copy( currentFile, newFile )
			print '>>>   "%s" is well compiled.' %( newFile )
		else:
			if '.pyc' in file:
				if folder == curentPath:
					newFile = '%s/%s' %( softwarePath, file )

				if os.path.exists( newFile ):
					os.remove( newFile )

				os.rename( currentFile, newFile )
				print '>>>   "%s" is well compiled.' %( newFile )

print '>>> Install End'