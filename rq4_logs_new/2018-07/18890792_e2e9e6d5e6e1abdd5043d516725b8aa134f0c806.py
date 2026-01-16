
'''
Given a imagename prefix and path, gather all saved channel outputs
and create a table. This functionality is built into
sequential_clean_submission.py, but is useful in a separate script
when gathering results in a cluster environment.

Returns an HDF5 file with the combined table of results.

Can be run in a normal python environment.

Requires numpy and astropy.
'''

import sys
import os
from glob import glob
import numpy as np
from astropy.table import Table, Column


path = sys.argv[1]

# W/o _channel_{num} ending
filename = sys.argv[2]

files = glob(os.path.join(path, filename, "_channel_*"))

# Sort by number
files.sort()

for i, fil in enumerate(files):

    out_dict = np.load(fil).item()

    # Setup a dictionary
    if i == 0:
        all_dict = dict.fromkeys(out_dict)

        for key in all_dict:
            all_dict[key] = []

    for key in out_dict:
        all_dict[key].append(out_dict[key])


# Now convert the dictionary into an astropy table

table = Table()

for key in all_dict:

    table[key] = Column(all_dict[key])

stop_codes = {0: "Not reached", 1: "Reached niter",
              2: "Reached threshold",
              3: "Stop flag", 4: "No change after cycle",
              5: "Diverging. Peak residual increase.",
              6: "Diverging. Peak min. residual increase.",
              7: "Empty clean mask",
              8: "Reached nsigma threshold"}

# Empty string array set to longest message in stop_codes
str_outs = np.empty(table['nsigma'].shape, dtype='S39')

str_outs = []

for code in table['stopcode']:

    if code < 0 or code > 8:
        raise ValueError("Found code {0} not within 0 and 8. Check"
                         " output.".format(code))

    str_outs.append(stop_codes[code])

table['stopcode_exp'] = Column(str_outs)

# Save the table in the given path

table.write(os.path.join(path, filename + ".h5"), path='data', format='hdf5')