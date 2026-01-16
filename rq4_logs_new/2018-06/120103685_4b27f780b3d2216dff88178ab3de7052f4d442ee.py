# Program to get array indices of unique mergers
# from /astro1/dunhamsj/nMergersHistogram/ChunkedMergersUnique/*

import numpy as np
from time import time
from datetime import datetime

for i in range( 1, 79 ):
    ChunkNumber = str(i)

    ##### File I/O
    ProgName = 'GetUniqueMergers.py'
    FileOut  = 'UniqueMergers_chunk' + ChunkNumber + '.dat'
    FileIn   = 'mergers_chunk' + ChunkNumber + '.dat'
    with open( FileOut, 'w' ) as f:
        f.write( '# {:}\n\n'.format(FileOut) )
        f.write( '# These data were obtained with {:}\n'.format(ProgName) )
        f.write( '# Create array containing data only from unique mergers\n' )
        f.write( '# Program start-time: {:}\n\n'.format(datetime.now()) )

    StartTime = time()

    ChunkedData = np.loadtxt( FileIn, dtype = 'float' )
    UniqueIndices = np.unique(ChunkedData[:,0], return_index = True)[1]

    EndTime = time()

    TotRunTime = EndTime - StartTime

    with open( FileOut, 'a' ) as f:
        f.write( '# Total number of mergers:        \
{:.10e}\n'.format( ChunkedData.shape[0] ) )
        f.write( '# Total number of unique mergers: \
{:.10e}\n\n'.format( UniqueIndices.shape[0] ) )
        f.write( '# Data structure:\n' )
        f.write( '# Merger ID, Snapshot Redshift, M1 [Msun], \
M2 [Msun], Delay Time [Gyr], Lookback Time [Gyr]\n' )
        np.savetxt( f, ChunkedData, fmt = '%d %.6e %.6e %.6e %.6e %.6e' )
        f.write( '\n# Program end-time: {:}\n'.format(datetime.now()) )
        f.write( '# Total run-time: {:.10e} s\n'.format(TotRunTime) )
