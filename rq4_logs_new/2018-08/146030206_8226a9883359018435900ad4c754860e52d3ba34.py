import os

# Config file that contains watershed extents by division/district.

CWMSGRID_HOME = os.getenv('CWMSGRID_HOME')

TOP_DIR = '{}/software/snowmelt_app'.format(CWMSGRID_HOME) # May need to change this - need to check what uses this

SRC_DIR = '{}/data/NOHRSC-snodas/data/data_raw'.format(CWMSGRID_HOME)
ARCHIVE_DIR = SRC_DIR
ARCHIVE_DIR_2012 = SRC_DIR

PROCESSED_SRC_DIR = 'data/NOHRSC-snodas/data/data_processed'.format(CWMSGRID_HOME)

# These three locations are the base directories for our output files.
DSS_BASE_DIR = '{}/data/NOHRSC-snodas/data/dss'.format(CWMSGRID_HOME)
HISTORY_BASE_DIR = '{}/data/NOHRSC-snodas/history'.format(CWMSGRID_HOME)
ASC_BASE_DIR = '{}/data/NOHRSC-snodas/data/data_being_processed_1'.format(CWMSGRID_HOME)

HEADER_KEY_DIR = '{}/software/snowmelt_app/key'.format(CWMSGRID_HOME)

KEEP_PROCESSED_SRC_DATA = False
SUBPROCESS_QUIET = False