import numpy as np

class Oscillator():
    def __init__(self, sr, initial_phase=None):

        self._sr = sr

        if not initial_phase:
            initial_phase = np.random.rand() * 2 * np.pi

        self._phase = initial_phase

        self._period_seconds = 1 / sr
        self._nyquist = sr // 2

    def tick(self, f0):

        assert f0 <= self._nyquist, "Requested frequency is above Nyquist."

        phase_advance = 2 * np.pi * f0 * self._period_seconds
        self._phase += phase_advance
        
        return np.cos(self._phase)