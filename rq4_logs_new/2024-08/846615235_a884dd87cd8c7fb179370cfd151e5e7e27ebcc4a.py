import pyaudio
import customtkinter as ctk
import math

# CustomTkinter initialization
screen_width = 500
screen_height = 500
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")
app = ctk.CTk()
app.geometry(f"{screen_width}x{screen_height}")
app.title("Realtime Wave Sign")

canvas = ctk.CTkCanvas(app, width=screen_width, height=screen_height, bg="black")
canvas.pack()

# pyaudio initialization
CHUNK = 1024
FORMAT = pyaudio.paInt16
CHANNELS = 1
RATE = 44100
p = pyaudio.PyAudio()
stream = p.open(format=FORMAT, channels=CHANNELS,
                rate=RATE, input=True,
                frames_per_buffer=CHUNK)

def get_microphone_input_level():
    data = stream.read(CHUNK)
    rms = 0
    for i in range(0, len(data), 2):
        sample = int.from_bytes(data[i:i+2], byteorder='little', signed=True)
        rms = rms + sample**2
    rms = math.sqrt(rms / CHUNK)
    return rms

def hex_to_rgb(hex_code):
    hex_code = hex_code.strip('#')
    hex_list = [int(hex_code[i:i+2], 16) for i in (0, 2, 4)]
    rgb_tuple = tuple(hex_list)
    return rgb_tuple

def draw_sine_wave(amplitude):
    canvas.delete("all")
    points = []
    if amplitude > 10:
        for x in range(0, screen_width):
            y = screen_height/2 + int(amplitude * math.sin(x * 0.02))
            points.append((x, y))
    else:
        points.append((0, screen_height/2))
        points.append((screen_width, screen_height/2))
    
    hex_code = '#5ce65c'
    color = hex_to_rgb(hex_code)
    canvas.create_line(points, fill=hex_code, width=2)

def update():
    amplitude_adjustment = get_microphone_input_level() / 50
    amplitude = max(10, amplitude_adjustment)
    draw_sine_wave(amplitude)
    app.after(17, update)  # Update at ~60 FPS

update()
app.mainloop()

# Cleanup
stream.stop_stream()
stream.close()
p.terminate()