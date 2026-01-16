from tkinter import *
from tkinter.ttk import Treeview
import random
import PIL
from PIL import ImageTk, ImageTk
from helpers.db_modifier import *
from helpers.helpers import create_tab, create_button, display_data_from_db, show_db_func, get_selected_npc_label_txt
from classes.NPC import NPC
from helpers.constants import *
import re
import tkinter as tk
import sys



class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('G2 NotR database')
        self.createWidgets()
        self.geometry("1280x720")
        self.iconbitmap('icon.ico')

    def createWidgets(self):
        self.my_notebook = ttk.Notebook()
        self.my_notebook.grid(pady=40, sticky="ns")
        # creating tabs
        self.edit_db_tab = create_tab(self.my_notebook, edit_db_text)
        self.browse_db_tab = create_tab(self.my_notebook, browse_db_text)
        self.all_npc_tab = create_tab(self.my_notebook, show_all_npcs_text)
        # creating buttons
        self.create_db_btn = create_button(self.edit_db_tab, create_db_text, create_db, 0)
        self.update_db_btn = create_button(self.edit_db_tab, update_db_text, update_db, 1)
        self.show_db_btn = create_button(self.edit_db_tab, show_db_text, self.show_db, 2)
        # creating tree
        self.npc_tree=ttk.Treeview(self.all_npc_tab, columns=column_names)
        self.npc_tree.place(x= 20, y= 0, width=1200, height=320)
        for idx, col in enumerate(column_names):
            wdth = 100 if idx == 0 or idx == len(column_names) - 1 else 60
            self.npc_tree.column(col, width=wdth, minwidth=wdth)
        self.npc_scroll = Scrollbar(self.all_npc_tab, orient=VERTICAL, command=self.npc_tree.yview)
        self.npc_scroll.place(x= 1220, y= 0, width=20, height=320)
        self.npc_tree.configure(yscrollcommand=self.npc_scroll.set)
        for col in column_names:
            self.npc_tree.heading(col, text=col, command=lambda _col=col: \
                     self.treeview_sort_column(self.npc_tree, _col, False))
        self.selected = ""
        self.selection_idx = 0
        self.npc_tree.bind('<<TreeviewSelect>>', self.on_select)
        self.select_npc_btn = Button(self.all_npc_tab, text=select_fighter_text, command=self.print_selected)
        self.select_npc_btn.place(x= 20, rely= 0.55, width=200, height=30)
        self.fight_btn = Button(self.all_npc_tab, text=fight_text, command=self.start_fight)
        self.fight_btn.place(x= 230, rely= 0.55, width=200, height=30)

        display_data_from_db(self.npc_tree)


    def show_db(self):
        show_db_func(self.edit_db_tab)

    def treeview_sort_column(self, treeview, col, reverse):
        l = [(treeview.set(k, col), k) for k in treeview.get_children('')]
        try:
            l.sort(key=lambda t: float(t[0]), reverse=reverse)
        except ValueError:
            l.sort(reverse=reverse)

        for index, (val, k) in enumerate(l):
            treeview.move(k, '', index)

        treeview.heading(col, command=lambda: self.treeview_sort_column(treeview, col, not reverse))     

    def on_select(self, event):
        self.selected = self.npc_tree.item(event.widget.selection())['text'].lower()
        
    def print_selected(self):
        if self.selection_idx == 0:
            self.selected_npc1 = NPC(self.selected)
            self.info_1 = Label(self.all_npc_tab, text=get_selected_npc_label_txt(self.selected_npc1) + "\nvs.\n")
            self.info_1.place(x=0, y=330, width=800, height=50)
        elif self.selection_idx == 1:
            self.selected_npc2 = NPC(self.selected)
            self.info_2 = Label(self.all_npc_tab, text=get_selected_npc_label_txt(self.selected_npc2))
            self.info_2.place(x=0, y=360, width=800, height=25)
        else: 
            self.info_3 = Label(self.all_npc_tab, text=two_selected_npc_error)
            self.info_3.place(x=0, y=380, width=250, height=25)
            self.all_npc_tab.after(5000, self.info_3.destroy)
        self.selection_idx += 1

    def clear_selection(self):
        self.selected_npc1 = ""
        self.selected_npc2 = ""
        self.selection_idx = 0
        self.info_1.destroy()
        self.info_2.destroy()
        self.fight_result.destroy()
        self.select_npc_btn = Button(self.all_npc_tab, text=select_fighter_text, command=self.print_selected)
        self.select_npc_btn.place(x= 20, rely= 0.55, width=200, height=30)    

    def start_fight(self):
        self.fight_result = Label(self.all_npc_tab, text=self.selected_npc1.fight(self.selected_npc2))
        self.selected_npc1.hp = self.selected_npc1.hp_max
        self.selected_npc2.hp = self.selected_npc2.hp_max
        self.fight_result.place(relx=0.05, rely=0.65, width=1250, height=150)
        self.select_npc_btn.destroy()
        self.new_fight_btn = Button(self.all_npc_tab, text=select_new_fighters_text, command=self.clear_selection)
        self.new_fight_btn.place(x=20, rely=0.55, width=200, height=30)