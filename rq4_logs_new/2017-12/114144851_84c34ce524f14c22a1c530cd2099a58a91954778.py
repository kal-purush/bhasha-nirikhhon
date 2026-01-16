import sys
import errno
import argparse
import pandas as pd
from IGV import *
'''
This is a command line interface for a Python interface with IGV via socket, written by Brent Pedersen.
It starts IGV and passes a series of arguments to it, allowing a series of snapshots of different regions for a series of strains to be taken.

example of usage:
python IGV-shell.py --genome WS245 --directory ~/snapshots --bams JT11398 --cendr True --regions II:1-1000 --snapshotnames test
'''



# try to connect to IGV, if it doesn't connect then try to start IGV and wait until it loads
started_igv = False
igv = False
while not igv:
	try:
	    igv = IGV()
	except socket.error, v:
	    errorcode=v[0]
	    if errorcode==errno.ECONNREFUSED:
	    	if not started_igv:
	    		print('Connection refused, assuming IGV not running, starting IGV')
	    		started_igv = True
	        	startIGV()
	        print('Waiting for IGV to load...')
	        time.sleep(1)

# initialize the parser
parser = argparse.ArgumentParser()

# add arguments to the parser
parser.add_argument("--command", nargs='*', help="run a command directly in IGV through the socket",type=str)
parser.add_argument("--genome", help="genome to load",type=str, default='WS245')
parser.add_argument("--directory", help="the directory to save snapshots in",type=str)
parser.add_argument("--bams", nargs='*', help=".bam files to load, or strain names to download from CeNDR. If downloading from CeNDR also pass '--cendr True'",type=str)
parser.add_argument("--tracks", nargs='*', help="names for the tracks, in the same order as the .bam argument",type=str)
parser.add_argument("--regions", nargs='*', help="regions to take snapshots of (in the format chromesome:start-stop (e.g. II:600-700), or gene name)",type=str)
parser.add_argument("--snapshotnames", nargs='*', help="names to save the snapshot of each region under, in the same order as the regions", type=str)
parser.add_argument("--cendr", help="fetch .bam and .bai URLs from the CeNDR database for each strain name in the bams argument", type=bool)

args = parser.parse_args()

# pass commands directly to the socket if specified
if args.command:
    for i in args.command:
        print(i)
        print(igv.send(i))

else:
    # load the genome, WS245 by default
    print('loading genome {}'.format(args.genome))
    print(igv.genome(args.genome))

    # if desired, fetch the .bam and .bai files using CeNDR:
    if args.cendr:
        # read in the table of download URLs
        strain_database = pd.read_table('https://www.elegansvariation.org/data/download/bam.sh', skiprows=7, header=None, sep=' ')
        strain_database = strain_database.ix[:,list(~strain_database.isnull().all())]
        # since you find the .bam on CeNDR using the strain name, change the track name to the strain name:
        args.tracks = args.bams
        # fetch each .bam and the corresponding .bai from CeNDR:
        for i in range(0,len(args.bams)):
            print('locating {} on CeNDR'.format(args.bams[i]))
            strain = args.bams[i]
            args.bams[i] = '{bam} index={index} name={name}'.format(bam=strain_database[strain_database[2] == strain + '.bam'][11].values[0], index=strain_database[strain_database[2] == strain + '.bam.bai'][7].values[0], name=strain)
            print('OK')

    # load in the tracks from the .bams and name them
    try:
        # load in the bams, and name the tracks based on the tracks argument:
        for url,name in zip(args.bams,args.tracks):
            print('loading bam {}'.format(url))
            print(igv.load('{url} name={name}'.format(url=url, name=name)))
    except TypeError:
        # if there is no --tracks argument, load .bams into tracks without setting track names:
        for url in args.bams:
            print('loading bam {}'.format(url))
            print(igv.load(url))

    # set the directory:
    if args.directory:
        print('setting snapshot directory to {}'.format(args.directory))
        print(igv.set_path(args.directory))

    # for each region that needs to be imaged go to it and save it.
    for region in args.regions:
        print('going to {}'.format(region))
        print(igv.go(region)) #cmd('load ' + bam + ' index=' + bai + ' name=' + strain + '-' + annotation)
        if args.snapshotnames:
            name = args.snapshotnames.pop()
            print('saving snapshot, {}'.format(name))
            print(igv.save(path=name))
        else:
            print('saving snapshot')
            print(igv.save())