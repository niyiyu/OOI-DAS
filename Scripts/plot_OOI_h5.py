'''
Scripts to plot OOI DAS data. Data are normalized to [0, 1].
OpenMPI is used to parallelize the job. Watch for the memory
    since each matrix is pretty big.

Usage:
    $ mpirun -np NPROC python plot_OOI_h5.py

Yiyu Ni (niyiyu@uw.edu)
Feb 9th, 2022
Dept. of Earth and Space Sciences
University of Washington
'''

from mpi4py import MPI
import gc
import h5py
import os
from datetime import datetime
import matplotlib.dates as mdates
import numpy as np
import matplotlib.pyplot as plt

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Refer to sermep.ess.washington.edu and check for the path.
path = "south-data-ejm/hdd/South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_2021-11-01T16_09_15-0700/"
fpath = '/home/niyiyu/notebooks/OOI-Figure/%s' % path
flist = os.listdir('/data/data2/' + path)
if rank == 0: 
    if not os.path.exists(fpath):
        os.makedirs(fpath)
comm.Barrier()

# Iterate through the file list
for idf, file in enumerate(flist):
    if idf % size == rank:
        f = h5py.File('/data/data2/' + path + file, 'r')
        
        # stdout
        print("Report from rank %d: working on No.%d H5 file. Time: %s" % (rank, idf, file.split("_")[-1]))
        data = f['Acquisition']['Raw[0]']['RawData'][:, :].astype('int64')
        timestamp = f['Acquisition']['Raw[0]']['RawDataTime'][:] / 1000000

        # Normalize the data into [0, 1]
        filt_normdata = np.zeros([47500, 12000])
        for i in range(47500):
            filt_normdata[i, :] = (data[i, :] - np.min(data[i, :])) / (np.max(data[i, :]) - np.min(data[i, :]))

        # Ploting
        x_lims = list(map(datetime.utcfromtimestamp, [timestamp[0], timestamp[-1]]))
        x_lims = mdates.date2num(x_lims)
        y_lims = [0, 47500]
        fig, ax = plt.subplots(figsize = (10, 30))
        plt.tight_layout()
        im = ax.imshow((filt_normdata[:, :]), extent = [x_lims[0], x_lims[1],  y_lims[0], y_lims[1]],  aspect = 1/20405363)
        ax.xaxis_date()
        plt.xlabel("Time", fontsize = 15)
        plt.ylabel("Channel", fontsize = 15)
        date_format = mdates.DateFormatter('%d-%b\n%H:%M:%S.%f')
        ax.xaxis.set_major_formatter(date_format)   
        plt.xticks(rotation=15)
        plt.xticks(np.linspace(x_lims[0], x_lims[1], 5))
        plt.yticks(np.linspace(0, 47500, 20))
        plt.title(file.split('.')[0])
        plt.colorbar(im, ax = ax, location = 'bottom', pad = 0.04)
        plt.savefig("%s%s.png" % (fpath, file.split('.')[0]), bbox_inches = 'tight')

        # Be sure to do this to save memory
        f.close()
        plt.close()
        del filt_normdata
        del data
        del timestamp
        gc.collect()
