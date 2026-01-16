from fabric.api import local

def deploy():
	local('pip freeze > requirements.txt')
	local('git add --ignore-removal .')
	print('git commit comment please - ')
	comment = raw_input()
	local('git commit -m "%s"'%comment )
	local("git push origin master")