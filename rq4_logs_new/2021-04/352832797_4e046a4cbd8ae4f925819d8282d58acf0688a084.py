
import speech_recognition as sr
import librosa
import soundfile as sf

from librosa.core import audio
from numpy import lib

import sys
sys.path.append('c:/nmb/nada/stt/voice_manipulate/')

from pitch_function import pitch_change

# pitch_change(
#     loaddir = 'c:/nmb/nmb_data/stt_predict/STT voice denoise',
#     n_steps = 2,
#     outdir = 'c:/nmb/nmb_data/stt_predict/octave_denoise/'
# )

r = sr.Recognizer()

txt = list()
f = open('c:/nmb/nmb_data/pitch_change.txt', 'w')

path_a = 'c:/nmb/nmb_data/stt_predict/octave/octave_up/octave_up_'
path_b = 'c:/nmb/nmb_data/stt_predict/octave/octave_down/octave_down_'
path_c = 'c:/nmb/nmb_data/stt_predict/octave_denoise/octave_up/octave_up_'
path_d = 'c:/nmb/nmb_data/stt_predict/octave_denoise/octave_down/octave_down_'

path_list = list([path_a, path_b, path_c, path_d])

# for j in path_list:
#     if j == path_a:
#         a = '[octave_up]'
#     elif j == path_b:
#         a = '[octave_down]'
#     elif j == path_c:
#         a = '[denoise_octave_up]'
#     else :
#         a = '[denoise_octave_down]'
#     f.write(a + '\n')
#     for i in range(1, 13):
#         audio = sr.AudioFile(j + str(i) + '.wav')
#         with audio as audio_file:
#             file = r.record(audio_file)
#         stt = r.recognize_google(file, language='ko-KR')
#         txt.append(stt)
#         f.write(stt + '\n')
#         print(stt)
#     f.write('\n')

# f.close()


for i in range(1, 13):
    for j in path_list:
        if j == path_a:
            a = '[octave_up]'
        elif j == path_b:
            a = '[octave_down]'
        elif j == path_c:
            a = '[denoise_octave_up]'
        else:
            a = '[denoise_octave_down]'
        audio = sr.AudioFile(j + str(i) + '.wav')
        with audio as audio_file:
            file = r.record(audio_file)
        stt = r.recognize_google(file, language='ko-KR')
        f.write(a + '\n' + stt + '\n')
        print(stt)
    f.write('\n')
f.close()