import h5py
import os
from datetime import datetime
import matplotlib.dates as mdates
import numpy as np
import matplotlib.pyplot as plt
import scipy
from time import perf_counter
import glob

path = "/data/data2/south-data-ejm/hdd/South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_2021-11-01T16_09_15-0700/"
filebase = 'South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_'
search = path + filebase + "2021-11-02T*"
filelist = glob.glob(search)
filelist.sort()
print(search)

print ('Merging %d files.'%len(filelist))

data = np.array([])
timestamp = np.array([])

channel_to_plot = 20000
t0 = perf_counter()

for i,file in enumerate(filelist):
    f = h5py.File(file, 'r')
    this_data = f['Acquisition']['Raw[0]']['RawData'][:, :].astype('int64')
    this_timestamp = f['Acquisition']['Raw[0]']['RawDataTime'][:] / 1000000
    data = np.append(data, this_data[channel_to_plot, :] )
    timestamp = np.append(timestamp,this_timestamp)
    
    print("loaded file %d of %d (%f s)"%(i,len(filelist),perf_counter()-t0))

with h5py.File(filebase + 'channel%d.h5'%channel_to_plot, 'w') as hf:
    hf.create_dataset('channel_%d'%channel_to_plot, data=data)
    hf.create_dataset('timestamp', data=timestamp)