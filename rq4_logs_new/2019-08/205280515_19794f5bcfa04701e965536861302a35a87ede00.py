import tkinter as Tk
import os, sys, inspect
# below 3 lines add the parent directory to the path, so that SQL_functions can be found.
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
parentdir = os.path.dirname(parentdir)
sys.path.insert(0, parentdir+'/sql_files/')
import selection
import datetime
import addel
from tkinter import font as tkFont


class CustomerWindow(Tk.Frame):
    def __init__(self, parent, **kwargs):
        Tk.Frame.__init__(self, parent, **kwargs)
        self.parent = parent
        self.notes_for_person_frame = Tk.Frame(self, borderwidth=1, relief='solid')
        self.person_notes = Tk.Text(self.notes_for_person_frame,
                                    borderwidth=1,
                                    width=65,
                                    height=20,
                                    wrap="word")
        self.title_font = tkFont.Font(size=16, weight='bold')
        self.basic_information_window = Tk.Frame(self, borderwidth=1, relief='solid')
        self.person_font = tkFont.Font(size=16, weight='bold')
        self.selection = selection.Selection()
        self.addel = addel.AdDel()

    def clear_customer_window(self):
        for widget in self.winfo_children():
            widget.destroy()
        self.basic_information_window = Tk.Frame(self, borderwidth=1, relief='solid')
        self.notes_for_person_frame = Tk.Frame(self, borderwidth=1, relief='solid')
        self.person_notes = Tk.Text(self.notes_for_person_frame,
                                    borderwidth=1,
                                    width=65,
                                    height=20,
                                    wrap="word")

    def generate_customerpage(self, customer):
        self.basic_information_window.grid(row=0,
                                           column=0,
                                           sticky=Tk.NW,
                                           padx=5,
                                           pady=5,
                                           ipadx=2,
                                           ipady=2)
        Tk.Label(self.basic_information_window,
                 text="Name: " + str(customer[1]),
                 font=self.person_font).grid(row=1, column=0, sticky=Tk.W)
        Tk.Button(self.basic_information_window,
                  text="Delete Job",
                  command=lambda: self.delete_job(customer[0]).grid(row=1, column=3, sticky=Tk.W))
        Tk.Label(self.basic_information_window, text="Type: " + str(customer[2])).grid(row=2, column=0, sticky=Tk.W)
        Tk.Label(self.basic_information_window, text="Organization: " + str(customer[3])).grid(row=4, column=0, sticky=Tk.W)
        Tk.Label(self.basic_information_window, text="Client Since: " + str(customer[4])).grid(row=5, column=0, sticky=Tk.W)

    def display_personal_notes(self, job):
        self.notes_for_person_frame.grid(row=2, column=0, rowspan=1, columnspan=3, sticky=Tk.NW, pady=5, padx=5, ipadx=2, ipady=2)
        try:
            for item in self.selection.select_latest_people_notes(str(job[0])):
                latest_job_note = item[2]
            self.person_notes.insert('end-1c', latest_job_note)
        except UnboundLocalError:
            bummer_note = "Add notes here."
            self.person_notes.insert('end-1c', bummer_note)
        Tk.Label(self.notes_for_person_frame, text="Person Notes", font=self.title_font).grid(row=0, column=0, sticky=Tk.W)
        Tk.Button(self.notes_for_person_frame, text="Update Notes", command=lambda: self.update_notes(job)).grid(row=3,
                                                                                                                 column=0,
                                                                                                                 pady=5,
                                                                                                                 sticky=Tk.W)
        self.person_notes.grid(row=1, column=0, sticky=Tk.W, padx=2, pady=2)

    def delete_job(self, id):
        self.addel.delete_person((id,))
        self.addel.delete_notes((id,))
        self.parent.display_searchpage()

    def update_notes(self, customer):
        entry = (customer[0],
                 self.person_notes.get("1.0", 'end-1c'),
                 datetime.date.today())
        self.addel.new_people_notes_table_entry(entry)
        self.parent.display_customerpage(customer)