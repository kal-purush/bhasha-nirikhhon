import numpy as np
import settings
from time import gmtime, strftime
TimeStamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())

rawDataDump = settings.RawDump;


def WriteArrayToFile(data, filename):
    TimeStamp = strftime("%Y%m%d-%H%M%S", gmtime())
    if(type(data) == list):
        data = np.array(data);
    #time stamp ensures that we don't overwite anything when we dump into raw data
    np.savetxt(rawDataDump+'\\'+filename+'_'+TimeStamp+'.csv', data, delimiter=",")
    print('success')

