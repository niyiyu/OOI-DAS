import h5py
from os.path import exists
import os
from datetime import datetime
import matplotlib.dates as mdates
import numpy as np
import matplotlib.pyplot as plt
import scipy
from time import perf_counter
import glob
from tqdm import tqdm
from scipy.signal import decimate

def merge_optasense(pth,filebase,outpth,nchan=47500):
    '''
    Given a directory path and a filename base that point to some optasense das hdf5 files,
    reorganize these files (which contain all channels in the array in one minute chunks) into
    files that contain all time steps for a single channel.
    '''
    search = pth + filebase + "*"
    filelist = glob.glob(search)
    filelist.sort()

    data = np.array([])
    timestamp = np.array([])

    t0 = perf_counter()
    for i,file in enumerate(filelist):
        with h5py.File(file, 'r') as f:
            print("Working with file %d of %d (%f s)."%(i,len(filelist),perf_counter()-t0))
            
            '''
            Loop over channels. Can this use multithreading?
            '''
        
            already_computed=np.arange(0,nchan,2**5)
            indices = np.setdiff1d(np.arange(0,nchan,2**4),already_computed)

            for channel_to_plot in tqdm(indices):
            
                new_data = f['Acquisition']['Raw[0]']['RawData'][channel_to_plot, :].astype('int64')
                new_timestamp = f['Acquisition']['Raw[0]']['RawDataTime'][:] / 1000000
                
                file_to_write = outpth + filebase + '_channel%d.h5'%channel_to_plot
                file_exists = exists(file_to_write)
                hf = h5py.File(file_to_write, 'a')

                # See https://stackoverflow.com/questions/47072859/how-to-append-data-to-one-specific-dataset-in-a-hdf5-file-with-h5py
                if file_exists:
                    hf['data'].resize((hf['data'].shape[0] + new_data.shape[0]), axis=0)
                    hf['data'][-new_data.shape[0]:] = new_data
                    hf['timestamp'].resize((hf['timestamp'].shape[0] + \
                                            new_timestamp.shape[0]),axis=0)
                    hf['timestamp'][-new_data.shape[0]:] = new_timestamp
                else:
                    hf.create_dataset('data', data=new_data,maxshape=(None,))
                    hf.create_dataset('timestamp', data=new_timestamp,maxshape=(None,))
                    
                    
                hf.close()
                    
def main():
    input_path = "/data/fast1/South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_2021-11-01T16_09_15-0700/"
    
    input_filebase = 'South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_'
    output_path = "/data/fast1/OOI-channel-files/"
    
    merge_optasense(input_path,input_filebase,output_path)
    
if __name__ == "__main__":
    main()
