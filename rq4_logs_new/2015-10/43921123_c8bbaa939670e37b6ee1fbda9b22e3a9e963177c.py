from tkinter import *

class Gui():
    """Gui object. input_callback is a function that determines what to do
    when send is clicked"""
    def __init__(self, input_callback):
        root = Tk()
        self.input_callback = input_callback

        # Output box
        self.output_label = Label(root, text='Output')
        self.output_label.pack(side=TOP)
        self.output_box = Text(root, height=20)
        self.output_box.config(state=DISABLED)
        self.output_box.pack(side=TOP, fill='both', expand=True)

        # Input box
        self.input_label = Label(root, text='Input')
        self.input_label.pack(side=TOP)
        self.input_box = Entry(root, width=30)
        self.input_box.pack(side=LEFT, fill='both', expand=True)
        self.send_button = Button(root, command=self.__on_send_clicked, text='Send')
        self.send_button.pack(side=RIGHT)

        root.mainloop()

    def __on_send_clicked(self):
        result = self.input_box.get()
        self.input_box.delete(0, 'end')
        self.input_callback(result)

    def add_output(self, output):
        self.output_box.config(state=NORMAL)
        self.output_box.insert(INSERT, '> ' + output + '\n')
        self.output_box.config(state=DISABLED)


if __name__ == '__main__':
    def do_stuff(text):
        print(text)

    gui = Gui(do_stuff)
    gui.add_output('hi')
    gui.add_output('hello')