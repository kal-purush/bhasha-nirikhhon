# -*- coding: utf-8 -*-
import pyaudio
import numpy as np
import glob
import os
import matplotlib.pyplot as plt
import time
from scipy.signal import chirp
import pandas as pd

def senoidal(f_sampleo=44100, frecuencia=100, num_puntos=1024, vpp=1.,
             offset=0.):
    """
    Genera una señal senoidal de frecuencia y numero de puntos definida
    
    Parameters
    ----------
    f_sampleo(float) = frecuencia de sampleo de la señal
    frecuencia(float) = frecuencia de la señal
    duracion(float) = duracion de la señal
    vpp(float) = valor pico a pico
    offset(float) = valor dc de la señal
    dtype = data type de la señal
    Returns
    -------
    Devuelve array
    """
    times = np.arange(num_puntos)
    return (vpp/2*(np.sin(2*np.pi*times*frecuencia/f_sampleo))
            + offset).astype(np.float32)


def take(arr, partlen):
    larr = len(arr)
    while True:
        cursor = 0
        while cursor < larr-partlen:
            tmp = arr[cursor:cursor+partlen]
            yield tmp
            cursor = min(cursor+partlen, larr+1)


def create_callback(gen):
    def callback_output(out_data, frame_count, time_info, status):
        out_data = next(gen)
        return out_data, pyaudio.paContinue
    return callback_output


def time_per_freq(freq_arr, per_per_freq):
    cursor = 0
    while cursor < len(freq_arr):
        yield per_per_freq/freq_arr[cursor]
        cursor += 1


# %% Barrido en frecuencia/caracterizacion par emisor-receptor
pa = pyaudio.PyAudio()
fs = 192000
CHUNK = 1024

volumen_inicial = .1
volumen_final = 3.
n_volumenes = 4
volumenes = np.geomspace(volumen_inicial, volumen_final, n_volumenes)

# Barriendo en frecuencia
frecuencia_inicial = 50
frecuencia_final = 44000
n_frecuencias = 50
frecuencias_log = np.geomspace(frecuencia_inicial, frecuencia_final, n_frecuencias)
frecuencias_caida = np.linspace(15000, 25000, 10)
frecuencias = np.concatenate((frecuencias_log, frecuencias_caida))
frecuencias = np.sort(frecuencias)
n_periodos_per_freq = 50

durations = time_per_freq(frecuencias, n_periodos_per_freq)

vout_vin = np.zeros((n_frecuencias, n_volumenes))
fouout_fouin = np.zeros((n_frecuencias, n_volumenes))
datos_serie = []
frecuencias_serie = []
volumenes_serie = []

df = pd.DataFrame(columns=['frecuencia (Hz)', 'amplitud enviada (num)', 
                           'amplitud leida (num)', 'leida/enviada'])
n_medicion = 1
for n_freq, freq in enumerate(frecuencias):
    t_medicion = next(durations)
    t_medicion = 0.5
    for n_vol, vol in enumerate(volumenes):
        tmp = senoidal(f_sampleo=fs, frecuencia=freq, num_puntos=CHUNK*100,
                       vpp=vol, offset=0.)
        stream_out = pa.open(format=pyaudio.paFloat32,
                             channels=1,
                             rate=fs,
                             output=True,
                             frames_per_buffer=CHUNK,
                             stream_callback=create_callback(take(tmp, CHUNK)))
        stream_out.start_stream()
        num_datos = int(t_medicion*fs)
        datos = np.zeros(num_datos)
        tiempo = np.arange(0, t_medicion, 1/fs)
        stream_in = pa.open(format=pyaudio.paFloat32,
                            channels=1,
                            rate=fs,
                            input=True,
                            frames_per_buffer=CHUNK)
        stream_in.start_stream()
        fin = CHUNK
        while fin < int(t_medicion*fs):
            new_data = stream_in.read(CHUNK)
            datos[fin-CHUNK:fin] = np.fromstring(new_data, 'Float32')
            fin += CHUNK
        stream_in.stop_stream()
        stream_out.stop_stream()
        to_analize = datos[-num_datos//10:]
        datos_serie.append(to_analize)
        frecuencias_serie.append(freq)
        volumenes_serie.append(vol)
        vin = (max(tmp)-min(tmp)) # lo que entra al circuito
        vout = (max(to_analize)-min(to_analize)) # lo que leemos
        fouin = max(np.abs(np.fft.fft(tmp)))
        fouout = max(np.abs(np.fft.fft(to_analize)))
        vout_vin[n_freq, n_vol] = vout/vin # leido / enviado
        fouout_fouin[n_freq, n_vol] = fouout/fouin
        df.loc[n_medicion] = [freq, vin, vout, vout/vin]
        n_medicion += 1
pa.terminate()
fig, ax = plt.subplots(1, sharex=True)
for n_vol, vol in enumerate(volumenes):
    ax.plot(frecuencias, vout_vin[:, n_vol], label='{}'.format(vol))
    ax.set_xlabel('vout/vin')
ax.plot(frecuencias, np.mean(vout_vin, axis=1), lw=2)
df.to_csv('barrido.dat')