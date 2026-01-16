import sys
import subprocess
import argparse
from argparse import RawTextHelpFormatter

def get_files(hash):
	return subprocess.check_output([ 'git', 'diff-tree', '--no-commit-id', '--name-only', '-r', hash ], universal_newlines=True).split('\n')

def main(first, second):

	print('Finding files in {}'.format(first))
	firstFiles = get_files(first)
	print('\tFound: {}'.format(len(firstFiles)))

	print('Finding files in {}'.format(second))
	secondFiles = get_files(second)
	print('\tFound: {}'.format(len(secondFiles)))

	print('')

	intersection = [f for f in firstFiles if f in secondFiles]

	for f in intersection:
		print(f)

if __name__ == '__main__':

	parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter, description='''Finds intersection of files between commits or commit ranges

Usage

	Create alias:

		git config --global alias.intersection '!python /path/to/git-intersection.py'

Examples:

	Find intersection between last two commits:

		git intersection master master~1

	Find intersection between two commit ranges

		git intersection e9b5c58..master e9b5c58..feat-1234

''')

	parser.add_argument('commits', metavar='COMMIT', nargs=2, help='Commits or commit ranges')

	args = parser.parse_args()

	main(*args.commits)