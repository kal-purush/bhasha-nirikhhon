from tkinter import *
from tkinter import ttk
import tkinter as tk
from tkinter import filedialog
import subprocess
import re

import paramiko
#! pip install paramiko


ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
host = ""
user = ""
password = ""
# archivo_seleccionado = StringVar()
ruta_absoluta = ""
root = Tk()
# root.resizable(0, 0)
menu_principal = ttk.Frame(root)
menu_conexion = ttk.Frame(root)
menu_compilar = ttk.Frame(root)
menu_ejecutar = ttk.Frame(root)
menu_eliminar = ttk.Frame(root)
menu_select_dir = ttk.Frame(root)
menu_cargar = ttk.Frame(root)
menu_select_cargar = ttk.Frame(root)


def compilar(source):
    stdin, stdout, stderr = ssh.exec_command('mpiLCIFunctions -s ' + source)
    result = stdout.read().decode().strip()

    # crear ventana para mostrar resultado
    resultado_window = tk.Toplevel()
    resultado_window.title("Resultado de Compilación")
    resultado_window.geometry("700x130")

    # crear widget Text y agregar resultado
    resultado_text = tk.Text(
        resultado_window, wrap="word", font=("Helvetica", 12))
    resultado_text.insert("1.0", result)
    resultado_text.pack(fill="both", expand=True)


def ejecutar(build, numproc):
    stdin, stdout, stderr = ssh.exec_command(
        'mpiLCIFunctions -b ' + build + ' -np ' + str(numproc))
    result = stdout.read().decode().strip()

    # crear ventana para mostrar resultado
    resultado_window = tk.Toplevel()
    resultado_window.title("Resultado de Ejecución")
    resultado_window.geometry("675x500")

    # crear widget Text y agregar resultado
    resultado_text = tk.Text(
        resultado_window, wrap="word", font=("Helvetica", 12))
    resultado_text.insert("1.0", result)
    resultado_text.pack(fill="both", expand=True)


def eliminar(dir, archivo, orden):
    stdin, stdout, stderr = ssh.exec_command(
        'mpiLCIFunctions ' + orden + ' ' + archivo)
    output = stdout.read().decode().strip()

    # crear ventana para mostrar resultado
    resultado_window = tk.Toplevel()
    resultado_window.title("Resultado de eliminación")
    resultado_window.geometry("325x80")
    # crear widget Text y agregar resultado
    resultado_text = tk.Text(
        resultado_window, wrap="word", font=("Helvetica", 12))

    if len(output) == 0:
        stdin, stdout, stderr = ssh.exec_command("rm " + dir + archivo)
        output = 'El archivo '+archivo + ' fue eliminado con éxito'

    resultado_text.insert("1.0", output)
    resultado_text.pack(fill="both", expand=True)


def cargar_archivo(dir, archivo, ruta_absoluta, orden):
    # stdin, stdout, stderr = ssh.exec_command(
    #     'mpiLCIFunctions ' + orden + ' ' + archivo)
    # output = stdout.read().decode().strip()

    # crear ventana para mostrar resultado
    resultado_window = tk.Toplevel()
    resultado_window.title("Resultado de Carga")
    resultado_window.geometry("325x80")
    # crear widget Text y agregar resultado
    resultado_text = tk.Text(
        resultado_window, wrap="word", font=("Helvetica", 12))

    if archivo == '':
        output = 'No se ha seleccionado ningún archivo'
    else:
        if orden == '-chs':
            if not re.search(r"\.(c|cpp)$", archivo):
                output = 'El archivo sebe de tener la extension .c/.cpp'
            else:
                # Construyendo el comando de scp
                comando_scp = "scp {} {}@{}:{}".format(
                    ruta_absoluta, user, host, dir)
                # Ejecutando el comando de scp en una terminal
                subprocess.run(comando_scp, shell=True, input=password,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                output = 'El archivo ' + archivo + 'Se cargo en el servidor con éxito'
        else:
            if not re.search(r"\.txt$", archivo):
                output = 'El archivo sebe de tener la extension .txt'
            else:
                # Construyendo el comando de scp
                comando_scp = "scp {} {}@{}:{}".format(
                    ruta_absoluta, user, host, dir)
                # Ejecutando el comando de scp en una terminal
                subprocess.run(comando_scp, shell=True, input=password,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                output = 'El archivo ' + archivo + 'Se cargo en el servidor con éxito'

    resultado_text.insert("1.0", output)
    resultado_text.pack(fill="both", expand=True)


def volver():
    menu_select_cargar.pack_forget()
    menu_select_dir.pack_forget()
    menu_ejecutar.pack_forget()
    menu_compilar.pack_forget()
    root.geometry("300x190")
    menu_principal.pack()


def volver_eliminar():
    menu_eliminar.pack_forget()
    root.geometry("300x190")
    menu_select_dir.pack()


def volver_cargar():
    menu_cargar.pack_forget()
    menu_select_cargar.pack()


def salir():
    ssh.close()
    root.destroy()


def connect_ssh():
    global host, user, password

    #! Borrar
    #!############################
    host = "172.23.208.188"
    user = "rockdri"
    password = "MetalMetal9219"
    #!############################

    try:
        # Conectar al servidor utilizando las credenciales necesarias
        ssh.connect(host, username=user, password=password)
        # Si la conexión es exitosa se muestra el menu principal
        crear_menu_principal()

    except Exception as e:
        print(e)
        print("Error de conexión")


def crear_menu_compilar():
    global menu_compilar, archivo_seleccionado
    root.title("Menu Compilar")
    root.geometry("500x175")
    menu_principal.pack_forget()
    menu_compilar = ttk.Frame(root)

    mostrar_directorio('$HOME/MPI/sourceMPI/', menu_compilar)

    archivo_seleccionado = Entry(menu_compilar, width=30)
    archivo_seleccionado.pack(pady=5)

    boton_compilar = Button(menu_compilar, text="Compilar",
                            command=lambda: compilar(archivo_seleccionado.get()))
    boton_compilar.pack(side=LEFT, padx=25)

    boton_regresar = Button(menu_compilar, text="Regresar", command=volver)
    boton_regresar.pack(side=LEFT)

    menu_compilar.pack()


def crear_menu_ejecutar():
    global menu_ejecutar, archivo_seleccionado
    root.title("Menu Ejecutar")
    root.geometry("500x175")
    menu_principal.pack_forget()
    menu_ejecutar = ttk.Frame(root)

    mostrar_directorio('$HOME/MPI/buildMPI/', menu_ejecutar)

    archivo_seleccionado = Entry(menu_ejecutar, width=30)
    archivo_seleccionado.pack(pady=5)

    slider_valor = IntVar()
    slider = Scale(menu_ejecutar, from_=1, to=128,
                   variable=slider_valor, orient=HORIZONTAL, length=300)
    slider.set(2)
    slider.pack(pady=5)

    boton_ejecutar = Button(menu_ejecutar, text="Ejecutar",
                            command=lambda: ejecutar(archivo_seleccionado.get(), slider_valor.get()))
    boton_ejecutar.pack(side=LEFT, padx=25)

    boton_regresar = Button(menu_ejecutar, text="Regresar", command=volver)
    boton_regresar.pack(side=LEFT)

    menu_ejecutar.pack()


def crear_menu_select_eliminar():
    global menu_select_dir
    # Creamos el frame del menu principal
    root.geometry("300x190")
    root.title("Seleccionar Directorio")

    menu_eliminar.pack_forget()
    menu_principal.pack_forget()
    menu_select_dir = ttk.Frame(root)

    # Creamos los elementos del menu principal
    ttk.Label(menu_select_dir, text="Seleccione un directorio:").pack(pady=10)
    ttk.Button(menu_select_dir, text="sourceMPI",
               command=lambda: crear_menu_eliminar('$HOME/MPI/sourceMPI/', '-chs')).pack()
    ttk.Button(menu_select_dir, text="buildMPI",
               command=lambda: crear_menu_eliminar('$HOME/MPI/buildMPI/', '-chb')).pack()
    ttk.Button(menu_select_dir, text="outputMPI",
               command=lambda: crear_menu_eliminar('$HOME/MPI/outputMPI/', '-cho')).pack()
    ttk.Button(menu_select_dir, text="machinefileMPI",
               command=lambda: crear_menu_eliminar('$HOME/MPI/machinefileMPI/', '-chm')).pack()
    ttk.Button(menu_select_dir, text="Regresar", command=volver).pack(pady=10)

    # Mostramos el menu principal
    menu_select_dir.pack()


def crear_menu_eliminar(dir, orden):
    global menu_eliminar, archivo_seleccionado
    root.title("Menu Eliminar - Directorio " + dir)
    root.geometry("500x190")
    menu_select_dir.pack_forget()
    menu_eliminar = ttk.Frame(root)

    mostrar_directorio(dir, menu_eliminar)

    archivo_seleccionado = Entry(menu_eliminar, width=30)
    archivo_seleccionado.pack(pady=5)

    boton_eliminar = Button(menu_eliminar, text="Eliminar",
                            command=lambda: eliminar(dir, archivo_seleccionado.get(), orden))
    boton_eliminar.pack(side=LEFT, padx=25)

    boton_regresar = Button(
        menu_eliminar, text="Regresar", command=volver_eliminar)
    boton_regresar.pack(side=LEFT)

    menu_eliminar.pack()

#################################################


def crear_menu_select_cargar():
    global menu_select_cargar

    root.geometry("300x150")
    root.title("Seleccionar Directorio")

    menu_cargar.pack_forget()
    menu_principal.pack_forget()
    menu_select_cargar = ttk.Frame(root)

    ttk.Label(menu_select_cargar, text="Seleccione un directorio:").pack(pady=10)
    ttk.Button(menu_select_cargar, text="sourceMPI",
               command=lambda: crear_menu_cargar('$HOME/MPI/sourceMPI/', '-chs')).pack()
    ttk.Button(menu_select_cargar, text="machinefileMPI",
               command=lambda: crear_menu_cargar('$HOME/MPI/machinefileMPI/', '-chm')).pack()
    ttk.Button(menu_select_cargar, text="Regresar",
               command=volver).pack(pady=10)

    menu_select_cargar.pack()


def crear_menu_cargar(dir, orden):
    global menu_cargar, archivo_seleccionado

    root.geometry("300x150")
    root.title("cargar en " + dir)
    menu_select_cargar.pack_forget()
    menu_cargar = ttk.Frame(root)

    def abrir_archivo():
        global archivo_seleccionado, ruta_absoluta
        archivo = filedialog.askopenfilename()
        archivo_seleccionado.set(archivo.split("/")[-1])
        ruta_absoluta = archivo

    archivo_seleccionado = StringVar()
    archivo_seleccionado.set("Ningún archivo seleccionado")
    Label(menu_cargar, textvariable=archivo_seleccionado).pack(pady=10)

    boton_abrir = Button(menu_cargar, text="Abrir",
                         command=abrir_archivo).pack()

    boton_cargar = Button(menu_cargar, text="Cargar",
                          command=lambda: cargar_archivo(dir, archivo_seleccionado.get(), ruta_absoluta, orden)).pack()

    boton_regresar = Button(
        menu_cargar, text="Regresar", command=volver_cargar).pack(pady=10)

    menu_cargar.pack()


#################################################


def mostrar_directorio(directorio, frame):

    def seleccionar_archivo(event):
        widget = event.widget
        seleccion = widget.curselection()
        archivo_seleccionado.delete(0, END)
        archivo_seleccionado.insert(END, widget.get(seleccion))

    stdin, stdout, stderr = ssh.exec_command('ls ' + directorio)
    archivos = stdout.read().splitlines()

    # Crear un Frame contenedor para el listbox y el scrollbar
    frame = Frame(frame)
    frame.pack(side=LEFT, fill=Y)

    # Crear el listbox y el scrollbar dentro del Frame
    lista_archivos = Listbox(frame, font=("Helvetica", 10), width=35)
    scrollbar = Scrollbar(frame, orient=VERTICAL)
    lista_archivos.config(yscrollcommand=scrollbar.set)
    scrollbar.config(command=lista_archivos.yview)

    # Empaquetar el listbox y el scrollbar dentro del Frame
    lista_archivos.pack(side=LEFT, fill=Y)
    scrollbar.pack(side=LEFT, fill=Y)

    # Insertar los archivos en el listbox
    if len(archivos) == 0:
        # lista_archivos.insert(END, "No hay archivos en el directorio")
        archivos = ['<Directorio Vació>']
        for archivo in archivos:
            lista_archivos.insert(END, archivo)
    else:
        for archivo in archivos:
            lista_archivos.insert(END, archivo.decode())
        # Asociar evento de selección de archivo a la función seleccionar_archivo()
        lista_archivos.bind('<ButtonRelease-1>', seleccionar_archivo)


def crear_menu_connect():
    global host, user, password, menu_conexion
    root.geometry("250x100")
    root.title("Sesión")
    menu_conexion = ttk.Frame(root)
    menu_conexion.pack()
    label_host = ttk.Label(menu_conexion, text="Host:")
    entry_host = ttk.Entry(menu_conexion)
    label_user = ttk.Label(menu_conexion, text="Username:")
    entry_user = ttk.Entry(menu_conexion)
    label_pass = ttk.Label(menu_conexion, text="Password:")
    entry_pass = ttk.Entry(menu_conexion, show="*")
    button_exec = ttk.Button(
        menu_conexion, text="Conectar", command=connect_ssh)
    text_output = Text(menu_conexion)
    # Posicionar los widgets
    label_host.grid(row=0, column=0, sticky="W")
    entry_host.grid(row=0, column=0, sticky="W", padx=80)
    label_user.grid(row=1, column=0, sticky="W")
    entry_user.grid(row=1, column=0, sticky="W", padx=80)
    label_pass.grid(row=2, column=0, sticky="W")
    entry_pass.grid(row=2, column=0, sticky="W", padx=80)
    button_exec.grid(row=3, column=0, sticky="W", padx=100)
    # text_output.grid(row=4, column=0)

    # Obtener los valores de los widgets
    host = entry_host.get()
    user = entry_user.get()
    password = entry_pass.get()


def crear_menu_principal():
    global menu_principal
    # Creamos el frame del menu principal
    root.geometry("300x190")
    root.title("Menu mpiLCI")

    menu_select_cargar.pack_forget()
    menu_select_dir.pack_forget()
    menu_conexion.pack_forget()
    menu_ejecutar.pack_forget()
    menu_compilar.pack_forget()
    menu_principal = ttk.Frame(root)

    # Creamos los elementos del menu principal
    ttk.Label(menu_principal, text="Seleccione una opción:").pack(pady=10)
    ttk.Button(menu_principal, text="Compilar",
               command=crear_menu_compilar).pack()
    ttk.Button(menu_principal, text="Ejecutar",
               command=crear_menu_ejecutar).pack()
    ttk.Button(menu_principal, text="Cargar",
               command=crear_menu_select_cargar).pack()
    ttk.Button(menu_principal, text="Eliminar",
               command=crear_menu_select_eliminar).pack()
    ttk.Button(menu_principal, text="Salir", command=salir).pack(pady=10)

    # Mostramos el menu principal
    menu_principal.pack()


crear_menu_connect()
root.mainloop()