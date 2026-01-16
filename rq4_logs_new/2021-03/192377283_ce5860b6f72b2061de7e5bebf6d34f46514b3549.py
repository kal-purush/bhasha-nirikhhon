import os
import time
import threading
from functools import partial


DEVICE = 'alsa_output.pci-0000_00_1f.3.analog-stereo.monitor'
COMMAND = 'pacat --record -d ' + DEVICE + ' | sox -t raw -r 44100 -L -e signed-integer -S -b 16 -c 2 - "output.wav"'


def wiretap(title, artist, album, length, delay):
    title, artist, album = title.get(), artist.get(), album.get()
    t = threading.Thread(target=os.popen, args=(COMMAND,))
    time.sleep(int(delay.get()))
    t.start()
    time.sleep(int(length.get()))
    os.popen('kill `pidof sox`')
    os.popen('kill `pidof pacat`')
    os.popen('mv output.wav '+title+'_'+artist+'_'+album+'.wav')


if __name__ == '__main__':

    import tkinter as tk

    root = tk.Tk()
    root.title('Wiretapper')

    title = tk.StringVar()
    artist = tk.StringVar()
    album = tk.StringVar()
    length = tk.StringVar()
    delay = tk.StringVar()

    tk.Label(root, text='Song Name:').grid(row=1, column=1)
    tk.Entry(root, bg='white', width=50, textvariable=title).grid(row=1, column=2)

    tk.Label(root, text='Artist:').grid(row=2, column=1)
    tk.Entry(root, bg='white', width=50, textvariable=artist).grid(row=2, column=2)

    tk.Label(root, text='Album:').grid(row=3, column=1)
    tk.Entry(root, bg='white', width=50, textvariable=album).grid(row=3, column=2)

    tk.Label(root, text='Length: (seconds)').grid(row=4, column=1)
    tk.Spinbox(root, from_=1, to=3600, textvariable=length).grid(row=4, column=2)

    tk.Label(root, text='Recording Delay: (seconds)').grid(row=5, column=1)
    tk.Spinbox(root, from_=0, to=60, textvariable=delay).grid(row=5, column=2)

    tk.Button(root, text='Wiretap!', command=partial(wiretap, title, artist, album, length, delay)).grid(row=6, column=1, columnspan=2)
    tk.Button(root, text='Quit', command=root.destroy)

    root.mainloop()