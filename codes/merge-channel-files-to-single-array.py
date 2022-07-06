import h5py
import os
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import butter, sosfilt, detrend, spectrogram, filtfilt
import glob
from scipy.signal.windows import tukey
from scipy.signal import decimate
from tqdm import tqdm
import datetime

def main():
    
    # Collect the file list
    pth = '/data/fast1/OOI-channel-files/'
    filebase = 'South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz__channel*.h5'
    files = glob.glob(pth + filebase)
    
    # Get the time and channel info
    f = h5py.File(files[0], 'r')
    data = np.array(f['timestamp'])
    times =list(map(datetime.datetime.fromtimestamp,np.array(f['timestamp'])))
    timestamp = np.array(f['timestamp'])
    f.close()

    channels=[]
    for file in files:
        channels.append(int(file.split('__channel')[1].split('.')[0]))
    channels = np.array(channels)
        
    # Make sure the data is read in thhe order. of increasing channel number
    sort_indices = np.argsort(channels)
    channels = channels [sort_indices]
    files = np.array(files) [sort_indices]
        
    duration_in_samples = len(times)
    sample_rate = 200
    secondsperday = 86400
    duration_in_days = duration_in_samples/sample_rate/secondsperday
    decimation_factor = 1000 # 1000 corresponds to 5s sampling
    max_distance = 95
    
    # Filter from 2 days to 5 seconds
    sos=butter(2,(1/86400/duration_in_days,0.2),'bandpass',fs=sample_rate,output='sos')

    # Filter from 5 minutes to 5 seconds
#     sos=butter(2,(1/300/duration_in_days,0.2),'bandpass',fs=sample_rate,output='sos')
    
    ''' 
    Merge the channel files into one big array
    '''
    data = np.zeros((int(duration_in_samples/decimation_factor),len(files)))
    for i,file in enumerate(tqdm(files)):
        f = h5py.File(file, 'r')

        data_filt = sosfilt(sos,np.array(f['data']))
        data[:,i] = data_filt[::decimation_factor]
        f.close()

    out_pth = '/data/fast1/OOI-channel-files/'
    # out_name='South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_372-Channels_2-d-5-s-filter.h5'
#     out_name='South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_745-Channels_5-min-5-s-filter.h5'
    # out_name='South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_745-Channels_2-d-5-s-filter.h5'
    out_name='South-C1-LR-95km-P1kHz-GL50m-SP2m-FS200Hz_1489-Channels_2-d-5-s-filter.h5'
    
    hf = h5py.File(out_pth+out_name, 'a')
    hf.create_dataset('data',      data=data)
    hf.create_dataset('timestamp', data=timestamp[::decimation_factor])
    hf.create_dataset('channels',  data=channels)
    hf.close()
    
if __name__ == "__main__":
    main()
