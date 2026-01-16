from tkinter import *
import sqlite3
import tkinter.ttk as ttk
import tkinter.messagebox as tkMessageBox

root = Tk()
root.title("Liste des contacts")
width = 700
height = 400
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()
x = (screen_width/2) - (width/2)
y = (screen_height/2) - (height/2)
root.geometry("%dx%d+%d+%d" % (width, height, x, y))
root.resizable(0, 0)
root.config(bg="#6666ff")

nom = StringVar()
prenom = StringVar()
adresse = StringVar()
num_tel = StringVar()



def Database():
    conn = sqlite3.connect("gestion_contact.db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS `contact` (contact_id INTEGER NOT NULL  PRIMARY KEY AUTOINCREMENT, prenom TEXT, nom TEXT, adresse TEXT, num_tel TEXT)")
    cursor.execute("SELECT * FROM `contact` ORDER BY `nom` ASC")
    fetch = cursor.fetchall()
    for data in fetch:
        tree.insert('', 'end', values=(data))
    cursor.close()
    conn.close()

def SubmitData():
    if  prenom.get() == "" or nom.get() == "" or adresse.get() == "" or num_tel.get() == "":
        result = tkMessageBox.showwarning('', 'Veuillez remplir tous les champs', icon="warning")
    else:
        tree.delete(*tree.get_children())
        conn = sqlite3.connect("gestion_contact.db")
        cursor = conn.cursor()
        cursor.execute("INSERT INTO `contact` (prenom, nom, adresse, num_tel) VALUES(?, ?, ?, ?)", (str(prenom.get()), str(nom.get()), str(adresse.get()), str(num_tel.get())))
        conn.commit()
        cursor.execute("SELECT * FROM `contact` ORDER BY `nom` ASC")
        fetch = cursor.fetchall()
        for data in fetch:
            tree.insert('', 'end', values=(data))
        cursor.close()
        conn.close()
        prenom.set("")
        nom.set("")
        adresse.set("")
        num_tel.set("")

def UpdateData():
    tree.delete(*tree.get_children())
    conn = sqlite3.connect("gestion_contact.db")
    cursor = conn.cursor()
    cursor.execute("UPDATE `contact` SET `prenom` = ?, `nom` = ?, `adresse` = ?, `num_tel` = ? WHERE `contact_id` = ?", (str(prenom.get()), str(nom.get()), str(adresse.get()), str(num_tel.get()), int(contact_id)))
    conn.commit()
    cursor.execute("SELECT * FROM `contact` ORDER BY `nom` ASC")
    fetch = cursor.fetchall()
    for data in fetch:
        tree.insert('', 'end', values=(data))
    cursor.close()
    conn.close()
    prenom.set("")
    nom.set("")
    adresse.set("")
    num_tel.set("")
        
    
def OnSelected(event):
    global contact_id, UpdateWindow
    curItem = tree.focus()
    contents =(tree.item(curItem))
    selecteditem = contents['values']
    contact_id = selecteditem[0]
    prenom.set("")
    nom.set("")
    adresse.set("")
    num_tel.set("")
    prenom.set(selecteditem[1])
    nom.set(selecteditem[2])
    adresse.set(selecteditem[3])
    num_tel.set(selecteditem[4])
    UpdateWindow = Toplevel()
    UpdateWindow.title("Liste des contacts")
    width = 400
    height = 300
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = ((screen_width/2) + 450) - (width/2)
    y = ((screen_height/2) + 20) - (height/2)
    UpdateWindow.resizable(0, 0)
    UpdateWindow.geometry("%dx%d+%d+%d" % (width, height, x, y))
    if 'NewWindow' in globals():
        NewWindow.destroy()

    FormTitle = Frame(UpdateWindow)
    FormTitle.pack(side=TOP)
    ContactForm = Frame(UpdateWindow)
    ContactForm.pack(side=TOP, pady=10)
    
    
    lbl_title = Label(FormTitle, text="Mettre à jour un contact", font=('arial', 16), bg="orange",  width = 300)
    lbl_title.pack(fill=X)
    lbl_firstname = Label(ContactForm, text="Prénom", font=('arial', 14), bd=5)
    lbl_firstname.grid(row=0, sticky=W)
    lbl_lastname = Label(ContactForm, text="Nom", font=('arial', 14), bd=5)
    lbl_lastname.grid(row=1, sticky=W)
    lbl_address = Label(ContactForm, text="Adresse", font=('arial', 14), bd=5)
    lbl_address.grid(row=2, sticky=W)
    lbl_contact = Label(ContactForm, text="Numéro de téléphone", font=('arial', 14), bd=5)
    lbl_contact.grid(row=3, sticky=W)

    firstname = Entry(ContactForm, textvariable=prenom, font=('arial', 14))
    firstname.grid(row=0, column=1)
    lastname = Entry(ContactForm, textvariable=nom, font=('arial', 14))
    lastname.grid(row=1, column=1)
    address = Entry(ContactForm, textvariable=adresse,  font=('arial', 14))
    address.grid(row=2, column=1)
    contact = Entry(ContactForm, textvariable=num_tel,  font=('arial', 14))
    contact.grid(row=3, column=1)
    

    btn_updatecon = Button(ContactForm, text="Mettre à jour", width=50, command=UpdateData)
    btn_updatecon.grid(row=4, columnspan=2, pady=10)


#fn1353p    
def DeleteData():
    if not tree.selection():
       result = tkMessageBox.showwarning('', 'Veuillez sélectionner un contact!', icon="warning")
    else:
        result = tkMessageBox.askquestion('', 'Êtes vous sûr de supprimer ce contact?', icon="warning")
        if result == 'yes':
            curItem = tree.focus()
            contents =(tree.item(curItem))
            selecteditem = contents['values']
            tree.delete(curItem)
            conn = sqlite3.connect("gestion_contact.db")
            cursor = conn.cursor()
            cursor.execute("DELETE FROM `contact` WHERE `contact_id` = %d" % selecteditem[0])
            conn.commit()
            cursor.close()
            conn.close()
    
def AddNewWindow():
    global NewWindow
    prenom.set("")
    nom.set("")
    adresse.set("")
    num_tel.set("")
    NewWindow = Toplevel()
    NewWindow.title("Liste des contacts")
    width = 400
    height = 300
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = ((screen_width/2) - 455) - (width/2)
    y = ((screen_height/2) + 20) - (height/2)
    NewWindow.resizable(0, 0)
    NewWindow.geometry("%dx%d+%d+%d" % (width, height, x, y))
    if 'UpdateWindow' in globals():
        UpdateWindow.destroy()
    
    #===================FRAMES==============================
    FormTitle = Frame(NewWindow)
    FormTitle.pack(side=TOP)
    ContactForm = Frame(NewWindow)
    ContactForm.pack(side=TOP, pady=10)

    
    #===================LABELS==============================
    lbl_title = Label(FormTitle, text="Créer un nouveau contact", font=('arial', 16), bg="#66ff66",  width = 300)
    lbl_title.pack(fill=X)
    lbl_firstname = Label(ContactForm, text="Prénom", font=('arial', 14), bd=5)
    lbl_firstname.grid(row=0, sticky=W)
    lbl_lastname = Label(ContactForm, text="Nom", font=('arial', 14), bd=5)
    lbl_lastname.grid(row=1, sticky=W)
    lbl_address = Label(ContactForm, text="Adresse", font=('arial', 14), bd=5)
    lbl_address.grid(row=2, sticky=W)
    lbl_contact = Label(ContactForm, text="Numero de téléphone", font=('arial', 14), bd=5)
    lbl_contact.grid(row=3, sticky=W)

    #===================ENTRY===============================
    firstname = Entry(ContactForm, textvariable=prenom, font=('arial', 14))
    firstname.grid(row=0, column=1)
    lastname = Entry(ContactForm, textvariable=nom, font=('arial', 14))
    lastname.grid(row=1, column=1)
    address = Entry(ContactForm, textvariable=adresse,  font=('arial', 14))
    address.grid(row=2, column=1)
    contact = Entry(ContactForm, textvariable=num_tel,  font=('arial', 14))
    contact.grid(row=3, column=1)
    

    #==================BUTTONS==============================
    btn_addcon = Button(ContactForm, text="Enregistrer", width=50, command=SubmitData)
    btn_addcon.grid(row=4, columnspan=2, pady=10)




    
#============================FRAMES======================================
Top = Frame(root, width=500, bd=1, relief=SOLID)
Top.pack(side=TOP)
Mid = Frame(root, width=500,  bg="#6666ff")
Mid.pack(side=TOP)
MidLeft = Frame(Mid, width=100)
MidLeft.pack(side=LEFT, pady=10)
MidLeftPadding = Frame(Mid, width=370, bg="#6666ff")
MidLeftPadding.pack(side=LEFT)
MidRight = Frame(Mid, width=100)
MidRight.pack(side=RIGHT, pady=10)
TableMargin = Frame(root, width=500)
TableMargin.pack(side=TOP)
#============================LABELS======================================
lbl_title = Label(Top, text="Gestion des contacts", font=('arial', 16), width=500)
lbl_title.pack(fill=X)

#============================ENTRY=======================================

#============================BUTTONS=====================================
btn_add = Button(MidLeft, text="+ Créer", bg="#66ff66", command=AddNewWindow)
btn_add.pack()
btn_delete = Button(MidRight, text="Supprimer", bg="red", command=DeleteData)
btn_delete.pack(side=RIGHT)

#============================TABLES======================================
scrollbarx = Scrollbar(TableMargin, orient=HORIZONTAL)
scrollbary = Scrollbar(TableMargin, orient=VERTICAL)
tree = ttk.Treeview(TableMargin, columns=("ContactID", "Prénom", "Nom", "Adresse", "Numéro de tél"), height=400, selectmode="extended", yscrollcommand=scrollbary.set, xscrollcommand=scrollbarx.set)
scrollbary.config(command=tree.yview)
scrollbary.pack(side=RIGHT, fill=Y)
scrollbarx.config(command=tree.xview)
scrollbarx.pack(side=BOTTOM, fill=X)
tree.heading('ContactID', text="ContactID", anchor=W)
tree.heading('Prénom', text="Prénom", anchor=W)
tree.heading('Nom', text="Nom", anchor=W)
tree.heading('Adresse', text="Adresse", anchor=W)
tree.heading('Numéro de tél', text="Numéro de tél", anchor=W)
tree.column('#0', stretch=NO, minwidth=0, width=0)
tree.column('#1', stretch=NO, minwidth=0, width=0)
tree.column('#2', stretch=NO, minwidth=0, width=80)
tree.column('#3', stretch=NO, minwidth=0, width=120)
tree.column('#4', stretch=NO, minwidth=0, width=120)
tree.column('#5', stretch=NO, minwidth=0, width=120)
tree.pack()
tree.bind('<Double-Button-1>', OnSelected)

#============================INITIALIZATION==============================
if __name__ == '__main__':
    Database()
    root.mainloop()
    