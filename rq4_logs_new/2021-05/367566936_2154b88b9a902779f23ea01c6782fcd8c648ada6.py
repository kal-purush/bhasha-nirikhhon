import tkinter


rows = ['qwertzuiopü', 'asdfghjklöä', 'yxcvbnm']

def shiftKeyboard(letter,shift):
    isUpper = letter.isupper()
    for l in rows:
        index = l.find(letter.lower())
        if(index == -1): continue
        decryptedIndex = (index+shift)%len(l)
        if(isUpper):
            return l[decryptedIndex].upper()
        return l[decryptedIndex]
    return letter

def encodeString(string):
    encoded = ""
    for i in range(0,len(string)):
        encoded += shiftKeyboard(string[i],1)
    return encoded

def decodeString(string):
    decoded = ""
    for i in range(0,len(string)):
        decoded += shiftKeyboard(string[i],-1)
    return decoded

def decodeChanged(text):
    sve.set(decodeString(svd.get()))

def encodeChanged(text):
    svd.set(encodeString(sve.get()))

window = tkinter.Tk()
window.title("GUI")

tkinter.Label(window, text = "Encode").grid(row = 0)
sve = tkinter.StringVar()
sve.trace("w", lambda name, index, mode, sve=sve: encodeChanged(sve))
encodeBox = tkinter.Entry(window, textvariable=sve)
encodeBox.grid(row = 0, column = 1)

tkinter.Label(window, text = "Decode").grid(row = 1)
svd = tkinter.StringVar()
svd.trace("w", lambda name, index, mode, svd=svd: decodeChanged(svd))
decodeBox = tkinter.Entry(window, textvariable=svd)
decodeBox.grid(row = 1, column = 1)

window.mainloop()