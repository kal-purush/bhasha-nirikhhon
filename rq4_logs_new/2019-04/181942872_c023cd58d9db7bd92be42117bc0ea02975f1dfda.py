import numpy as np
import scipy.signal
from util import chunk, pad_axis

def sampling_rate(a, t=None):
    """
        Return the mean sampling rate for a group of time-series measurements.
        Parameters
        ===========
        a: array-like
            Array of time-series measurements
        t: array-like or number or None, default=None
            If number, the average time per set of measurements.
            If array-like, the times at which each value in `a` was measured.
            If None, defaults to a.shape[-1]
    """
    if np.iterable(t):
        t_ptp = np.ptp(t, axis=-1)
        avg_time = t_ptp.mean()
    else:
        avg_time = a.shape[-1] if t is None else t 
    fs = a.shape[-1] / avg_time
    return fs

def stft(a, nperseg=256, noverlap=None, window='hann', center=False, pad=True, norm=False, axis=-1):
    """
        Perform a Short-Time Fourier Transform along the specified axis. 
        Equivalent to scipy.signal.stft except slightly faster (~10%) with modified usage/behavior.
        Parameters
        ===========
        a: array-like
            Array of time-series measurements
        nperseg: int, default=256
            Length of each segment
        noverlap: int or None, default=None
            Number of overlapping items in each segment. If None, defaults to nperseg // 2.
        window: str or None, default='hann'
            The window to use. If set to None, values in each segment are weighted equally.
            See scipy.signal.get_window documentation for more options.
        center: bool, default=False
            Whether to center the signal. If True, the signal is zero-padded by nperseg // 2 on each end.
        pad: bool, default=True
            Whether to pad the signal to fit an integer number of segments. If True, the end of the signal is zero padded.
        norm: bool, default=False
            Whether to use the 'ortho' norm during the fourier transform. See np.fft for details.
        axis: int, default=-1
            The axis along which the fourier transform is applied.

        Returns
        ==========
        output: np.ndarray
            The transformed array. The `axis` axis corresponds to the segment times.
    """
    if noverlap is None:
        noverlap = nperseg // 2
        
    if window is None:
        window = np.ones(nperseg, dtype=a.dtype)
    elif window == 'hann':
        window = np.hanning(nperseg + 1)[:-1]
    else:
        window = scipy.signal.get_window(window, nperseg)
    
    if center:
        a = pad_axis(a, 0, nperseg // 2, axis=axis)
        a = pad_axis(a, 0, nperseg // 2, axis=axis, append=True)
    
    segments = chunk(a, nperseg, noverlap, axis, pad=pad) * window
    
    output = np.fft.rfft(segments, norm='ortho' if norm else None)
    
    return output / sum(window)
    