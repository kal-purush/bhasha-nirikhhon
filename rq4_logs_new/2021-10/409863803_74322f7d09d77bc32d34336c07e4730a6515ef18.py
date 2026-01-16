from tkinter import *

root = Tk()

entry = Entry(root, width=50)
entry.pack()
entry.insert(0, "enter input")
entry.grid(row=0, column=0, columnspan=4)

# base_encode_button = Button(root, text="Submit", command=base64_decode,padx=50, pady=0)
# base_encode_button.grid(row=0, column=4)

# base_decode_button = Button(root, text="Submit", command=submit_click,padx=50, pady=0)
# base_decode_button.grid(row=0, column=4)

# download_button = Button(root, text="Submit", command=submit_click,padx=50, pady=0)
# download_button.grid(row=0, column=4)

def submit_click():
    output = Label(root, text=entry.get())
    output.pack()

submit_button = Button(root, text="Submit", command=submit_click,padx=50, pady=0)
submit_button.grid(row=0, column=4)

root.mainloop()
