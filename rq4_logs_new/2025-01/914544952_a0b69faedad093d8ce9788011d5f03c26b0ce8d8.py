import datetime
import time
import tkinter as tk
from tkinter import messagebox, ttk, filedialog
import threading
import pygame  # For playing sound

# Global Variables
alarm_time = None
alarm_active = False
selected_sound = None
snooze_duration = 5  # Snooze duration in minutes

# Initialize pygame mixer for sound
pygame.mixer.init()

# Function to update the current time on the GUI
def update_clock():
    current_time = datetime.datetime.now().strftime("%H:%M:%S")
    clock_label.config(text=current_time)
    clock_label.after(1000, update_clock)

# Function to set the alarm
def set_alarm():
    global alarm_time, alarm_active
    if not selected_sound:
        messagebox.showerror("Error", "Please select an alarm sound first.")
        return
    alarm_time = f"{hour_var.get()}:{minute_var.get()}:{second_var.get()}"
    alarm_active = True
    messagebox.showinfo("Alarm Set", f"Alarm set for {alarm_time}")

# Function to snooze the alarm
def snooze_alarm():
    global alarm_time, alarm_active
    current_time = datetime.datetime.now()
    snooze_time = current_time + datetime.timedelta(minutes=snooze_duration)
    alarm_time = snooze_time.strftime("%H:%M:%S")
    alarm_active = True
    messagebox.showinfo("Snoozed", f"Alarm snoozed to {alarm_time}")
    stop_sound()

# Function to stop the alarm
def stop_alarm():
    global alarm_active
    alarm_active = False
    stop_sound()
    messagebox.showinfo("Alarm Stopped", "Alarm has been stopped.")

# Function to select an alarm sound
def select_sound():
    global selected_sound
    sound_file = filedialog.askopenfilename(
        title="Select Alarm Sound",
        filetypes=(("Audio Files", "*.mp3 *.wav"), ("All Files", "*.*"))
    )
    if sound_file:
        selected_sound = sound_file
        sound_label.config(text=f"Selected: {sound_file.split('/')[-1]}")

# Function to play the alarm sound
def play_sound():
    if selected_sound:
        pygame.mixer.music.load(selected_sound)
        pygame.mixer.music.play(-1)  # Play in loop

# Function to stop the alarm sound
def stop_sound():
    pygame.mixer.music.stop()

# Function to check the alarm
def check_alarm():
    global alarm_active
    while True:
        if alarm_active and datetime.datetime.now().strftime("%H:%M:%S") == alarm_time:
            alarm_active = False
            play_sound()
            messagebox.showinfo("Alarm", "Time to Wake Up!")
        time.sleep(1)

# Start the alarm check in a separate thread
def start_alarm_thread():
    alarm_thread = threading.Thread(target=check_alarm, daemon=True)
    alarm_thread.start()

# Create GUI
root = tk.Tk()
root.title("Advanced Alarm Clock")
root.geometry("500x400")
root.resizable(False, False)
root.config(bg="#282C34")

# Clock Display
clock_label = tk.Label(
    root, text="", font=("Helvetica", 48), fg="#61AFEF", bg="#282C34"
)
clock_label.pack(pady=20)
update_clock()

# Alarm Time Selection
frame = tk.Frame(root, bg="#282C34")
frame.pack(pady=10)

tk.Label(frame, text="Set Alarm:", font=("Helvetica", 14), fg="white", bg="#282C34").grid(
    row=0, column=0, padx=5
)

hour_var = tk.StringVar(value="00")
minute_var = tk.StringVar(value="00")
second_var = tk.StringVar(value="00")

hour_menu = ttk.Combobox(frame, textvariable=hour_var, values=[f"{i:02}" for i in range(24)], width=5)
hour_menu.grid(row=0, column=1)
tk.Label(frame, text=":", font=("Helvetica", 14), fg="white", bg="#282C34").grid(row=0, column=2)

minute_menu = ttk.Combobox(frame, textvariable=minute_var, values=[f"{i:02}" for i in range(60)], width=5)
minute_menu.grid(row=0, column=3)
tk.Label(frame, text=":", font=("Helvetica", 14), fg="white", bg="#282C34").grid(row=0, column=4)

second_menu = ttk.Combobox(frame, textvariable=second_var, values=[f"{i:02}" for i in range(60)], width=5)
second_menu.grid(row=0, column=5)

# Alarm Control Buttons
set_button = tk.Button(
    root, text="Set Alarm", command=set_alarm, bg="#98C379", fg="black", width=15, font=("Helvetica", 12)
)
set_button.pack(pady=10)

snooze_button = tk.Button(
    root, text="Snooze", command=snooze_alarm, bg="#E5C07B", fg="black", width=15, font=("Helvetica", 12)
)
snooze_button.pack(pady=5)

stop_button = tk.Button(
    root, text="Stop", command=stop_alarm, bg="#E06C75", fg="white", width=15, font=("Helvetica", 12)
)
stop_button.pack(pady=5)

# Sound Selection
sound_button = tk.Button(
    root, text="Select Sound", command=select_sound, bg="#61AFEF", fg="white", width=15, font=("Helvetica", 12)
)
sound_button.pack(pady=10)

sound_label = tk.Label(
    root, text="No sound selected", font=("Helvetica", 10), fg="white", bg="#282C34"
)
sound_label.pack(pady=5)

# Start the alarm thread
start_alarm_thread()

# Run the GUI loop
root.mainloop()