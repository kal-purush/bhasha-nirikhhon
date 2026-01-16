import os
from random import *

fld = "dir"

for root, subdirs, files in os.walk(fld):
     for name in files:
          curr_fld = os.path.basename(root)
          oldname = os.path.join(fld, curr_fld, name)
          splt_name =  name.split('.')	
          myname = '_'.join([curr_fld, 'photo', str(randint(1, 10000)) + '.' + splt_name[1]])
          newname = os.path.join(fld, curr_fld, myname)
          os.rename(oldname, newname)