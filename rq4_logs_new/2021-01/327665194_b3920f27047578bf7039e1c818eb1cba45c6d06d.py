import tkinter as tk, os

dirname = os.path.dirname(__file__)
path = os.path.join(dirname, 'files to change')

window = tk.Tk(className = 'extention changer')
window.geometry('800x250')

text_box_frame = tk.Frame(master = window, width = 30, height = 10)
extention_to_change_to = tk.Entry(width = 30)

def change_extention(ending_extention):

  f = []

  for (dirpath, dirnames, filenames) in os.walk(path):
    
    f.extend(filenames)
  
    j = 0

    for x in filenames:

      file = filenames[j]
      splitat = file.rfind('.') + 1

      r = file[splitat:]
      length_to_remove = len(r)
      file = file[:-length_to_remove]

      file += ending_extention
      os.rename(path + '/' + filenames[j] , path + '/' + file)

      j += 1

    break

def handle_button_press(event): 
  change_extention(extention_to_change_to.get())

button_frame = tk.Frame(master = window, width = 20, height = 10)

convert_button = tk.Button(master = button_frame, text = 'CONVERT!!!')
convert_button.bind('<Button-1>', handle_button_press)

instructions_frame = tk.Frame(master = window, width = 200, height = 200)

instructions = tk.Label(master = instructions_frame, text = 'Place the files you want to change the extention of in the "files to change" folder.\nAll of those file extensions will be changed to the extention you choose by typing in the box, when you press the button.\nExample of what you would type in the box, "png", "jpeg", "txt".\nKindly note the lack of "."')

instructions_frame.pack(padx = 1, pady = 4)
button_frame.pack(padx = 1, pady = 1)
text_box_frame.pack(padx = 1, pady = 4)

instructions.pack()
convert_button.pack()
extention_to_change_to.pack()

window.mainloop()