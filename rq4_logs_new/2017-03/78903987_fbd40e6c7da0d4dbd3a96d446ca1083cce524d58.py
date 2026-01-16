from optparse 	import OptionParser

import csv
import sys

# GLOBALS
BITMAP_FREE_BLOCKS 	= []
FREE_BLOCKS 		= []
INVALID_BLOCKS		= []
BITMAP_FREE_INODES 	= []
FREE_INODES 		= []
INVALID_DIR_ENTRY 	= []

IN_USE_BLOCKS 		= {}
IN_USE_INODES 		= {}
IN_USE_DIR 		= {}
INDIRECT_BLOCKS 	= {}
UNALLOCATED_INODE 	= {}

# CLASS DECLARATIONS
class Block():
	def __init__(self, blocks, inodes, entry, indirect_block):
		self.mBlockEntry 	= blocks
		self.mInodeEntry 	= inodes
		self.mEntryNum 		= entry
		self.mIndirectBlock 	= indirect_block
		self.mReferencePointers = []

class Inode():
	def __init__(self, inode, link):
		self.mInodeEntry	= inode
		self.mLinkCount		= link
		self.mDirEntries	= []
		self.mBlockPointers	= []

class Directory():
	def __init__(self, inode, parent, entry, entry_name):
		self.mInodeEntry 	= inode
		self.mParentEntry	= parent
		self.mEntryNum		= entry
		self.mEntry_name	= entry_name

# FUNCTIONS
def getFileSystemData():
	# Open the files that we need
	super_csv = csv.reader(open('super.csv', 'rb'), 
		delimiter=',', 
		quotechar='"')
	group_csv = csv.reader(open('group.csv', 'rb'), 
		delimiter=',', 
		quotechar='"')
	bitmap_csv = csv.reader(open('bitmap.csv', 'rb'),
		delimiter=',')

	# Parse data for super.csv
	for line in super_csv:
		global MAGIC_NUMBER
		global TOTAL_BLOCKS
		global TOTAL_INODES
		global BLOCK_SIZE
		global FRAGMENT_SIZE
		global BLOCKS_PER_GROUP
		global INODES_PER_GROUP
		global FRAGMENTS_PER_GROUP
		global FIRST_DATA_BLOCK

		MAGIC_NUMBER		= int(line[0], 16)	# Convert from hex
		TOTAL_INODES		= int(line[1])
		TOTAL_BLOCKS		= int(line[2])
		BLOCK_SIZE		= int(line[3])
		FRAGMENT_SIZE		= int(line[4])
		BLOCKS_PER_GROUP	= int(line[5])
		INODES_PER_GROUP	= int(line[6])
		FRAGMENTS_PER_GROUP	= int(line[7])
		FIRST_DATA_BLOCK	= int(line[8])

	if DEBUGGING:
		print "SUPERBLOCK: %d, %d, %d, %d, %d, %d, %d, %d, %d" % (MAGIC_NUMBER, 
			TOTAL_INODES, 
			TOTAL_BLOCKS,
			BLOCK_SIZE,
			FRAGMENT_SIZE,
			BLOCKS_PER_GROUP,
			INODES_PER_GROUP,
			FRAGMENTS_PER_GROUP,
			FIRST_DATA_BLOCK)

	# Parse data for group.csv
	for line in group_csv:
		global IN_USE_BLOCKS
		global BITMAP_FREE_BLOCKS
		global BITMAP_FREE_INODES

		free_block_bitmap = int(line[5], 16)
		free_inode_bitmap = int(line[4], 16)
		BITMAP_FREE_BLOCKS.append(free_block_bitmap)
		BITMAP_FREE_INODES.append(free_inode_bitmap)
		IN_USE_BLOCKS[free_block_bitmap] = Block(free_block_bitmap, 0, 0, 0)
		IN_USE_BLOCKS[free_inode_bitmap] = Block(free_inode_bitmap, 0, 0, 0)

	# Parse data for bitmap.csv
	for line in bitmap_csv:
		global FREE_BLOCKS
		global FREE_INODES

		free_number = int(line[1])
		map_number = int(line[0], 16)

		if map_number in BITMAP_FREE_BLOCKS:
			FREE_BLOCKS.append(free_number)
		elif map_number in BITMAP_FREE_INODES:
			FREE_INODES.append(free_number)
		elif DEBUGGING:
			print "Free block/inode number does not appear in neither bitmaps.\n"

	return

def appendIN_USE_BLOCKS(block_num, inode_num, entry, indirect_num):
	global TOTAL_BLOCKS
	global IN_USE_BLOCKS
	global INVALID_BLOCKS

	if block_num == 0 or block_num > TOTAL_BLOCKS:
		INVALID_BLOCKS.append((block_num, inode_num, indirect_num, entry))
	else:
		if block_num not in IN_USE_BLOCKS:
			IN_USE_BLOCKS[block_num] = Block(block_num, inode_num, entry, indirect_num)
		IN_USE_BLOCKS[block_num].mReferencePointers.append((inode_num, indirect_num, entry))

	return

def checkInodes():
	global TOTAL_BLOCKS
	global INVALID_BLOCKS
	global IN_USE_INODES

	inode_csv = csv.reader(open('inode.csv', 'rb'),
		delimiter=',')

	for line in inode_csv:
		block_pointers = []
		inode = int(line[0])
		link_count = int(line[5])
		num_blocks = int(line[10])
		pending = num_blocks - 12
		IN_USE_INODES[inode] = Inode(inode, link_count)

		#
		for ptr in range(11, 11 + min(12, num_blocks)):
			appendIN_USE_BLOCKS(int(line[ptr], 16), inode, ptr-11, 0)

		for i in range(1, 4):

		## TO DO:

	return

def checkIndirect():
	global INDIRECT_BLOCKS

	indirect_csv = csv.reader(open('indirect.csv', 'rb'),
		delimiter=',')

	for line in indirect_csv:
		contains 	= int(line[0], 16)
		entry_num 	= int(line[1])
		block_pointer	= int(line[2], 16)

		if contains not in INDIRECT_BLOCKS:
			INDIRECT_BLOCKS[contains] = [(entry_num, block_pointer)]
		else:
			INDIRECT_BLOCKS[contains].append((entry_num, block_pointer))

	return

# MAIN
def main():
	# Globals
	global DEBUGGING

	# Option parser
	version_msg = "lab3b.py 1.0"
	usage_msg = "Reports incorrect blocks/inodes in file system."

	# Add option for debugging
	parser = OptionParser(version=version_msg, usage=usage_msg)
	parser.add_option("-d", "--debugging", 
		action="store_true", 
		dest="debugging", 
		default=False)
	options, args = parser.parse_args(sys.argv[1:])
	DEBUGGING = options.debugging

	# Run helper functions here
	getFileSystemData()
	checkIndirect()

if __name__ == "__main__":
	main()