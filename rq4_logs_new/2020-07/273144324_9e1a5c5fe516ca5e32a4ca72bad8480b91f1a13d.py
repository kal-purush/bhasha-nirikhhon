import datetime
import sounddevice as sd
from scipy.io import wavfile

samplerate = 16000 #Hz
channels = 1
duration = 5 # seconds

# sounddevie default values
sd.default.samplerate = samplerate
sd.default.channels = channels 

# Record returns the NumPy array for further processing given a time duration in seconds
def record(duration):
    recording = sd.rec(int(duration * samplerate), samplerate = samplerate, channels = 1)
    sd.wait()
    return recording

# Play the NumPy Array, not the wav file
def play(recording):
    sd.play(recording, samplerate)
    sd.wait() # Blocks untile audio is finished
    #sd.stop() # Stops audio

# Save Numpy Array to disk with scipy.io.wavfile
def save(recording):
    sd.wait()
    time = datetime.datetime.now()
    filename = 'recording' + time.strftime("%Y%m%d-%I-%M") + '.wav'
    wavfile.write(filename, samplerate, recording)

def main():
    myrecording = record(duration)
    play(myrecording)
    save(myrecording)


if __name__ == "__main__":
    main()