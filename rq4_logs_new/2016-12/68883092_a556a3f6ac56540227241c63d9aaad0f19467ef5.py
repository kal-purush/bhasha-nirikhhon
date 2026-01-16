import os, sys
curr_dir = os.getcwd().split('/')
sys.path.append('/'.join(curr_dir[:-1]))
ts_dir = curr_dir[:-1]
ts_dir.append('timeseries')
sys.path.append('/'.join(ts_dir))
from timeseries.StorageManagerInterface import StorageManagerInterface
from timeseries.ArrayTimeSeries import ArrayTimeSeries
from timeseries.SizedContainerTimeSeriesInterface import SizedContainerTimeSeriesInterface
from timeseries.TimeSeriesInterface import TimeSeriesInterface
from timeseries.FileStorageManager import FileStorageManager
from pytest import raises
import json
import numpy as np
import os

# smoke test to test if store and get can work
def smoke_test():
	fsm = FileStorageManager()
	arrayTS = ArrayTimeSeries([0,1,2,3],[4,5,6,7])
	fsm.store(1, arrayTS)
	assert fsm.get(1) == ArrayTimeSeries([0,1,2,3],[4,5,6,7])

# test get function when we try to get a file that is not existed
def test_no_id():
	fsm = FileStorageManager()
	arrayTS = ArrayTimeSeries([0,1,2,3],[4,5,6,7])
	fsm.store(1, arrayTS)
	with raises(Exception):
		fsm.get(4)

# test size function
def test_size():
	fsm = FileStorageManager()
	arrayTS = ArrayTimeSeries([0,1,2,3],[4,5,6,7])
	fsm.store(1, arrayTS)
	assert fsm.size(1) == 4