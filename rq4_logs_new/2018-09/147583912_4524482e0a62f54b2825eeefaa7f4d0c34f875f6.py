################################################################################
##      Editable variables
################################################################################
# Root from which to search for git repos
strSearchRoot = '/bhs00p10/Scripts'

import os
from subprocess import check_output

strRepoError = "Invalid GIT repository"

for dirName, subdirList, fileList in os.walk(strSearchRoot):
	if dirName.endswith("/.git"):
#		print('Found git directory %s' % dirName)
		# strip .git and use that folder
		dirToProcess = dirName[:-4]
#		print('Will diff directory %s' % dirToProcess)
		try:
			strResults = check_output(["bash", "-c" , 'cd %s;git diff-files' %dirToProcess])
		except:
			strResults = strRepoError
	
		if not strResults:
			pass
#			print('%s has no uncommitted changes' % dirToProcess)
		elif strResults == strRepoError:
			print('Invalid git repository at %s' % dirToProcess)
		elif len(strResults) >= 1:
			print('%s has uncommitted changes' % dirToProcess)