from tkinter import CENTER
from tkinter import font
import PySimpleGUI as sg

sg.LOOK_AND_FEEL_TABLE['Tema'] = {'BACKGROUND': '#FFAFCA',
                                      'TEXT': '#000000', 
                                        'INPUT': '#FFFFFF', 
                                        'TEXT_INPUT': '#000000', 
                                        'SCROLL': '#000000', 
                                        'BUTTON': ('#FFFFFF', '#000000'), 
                                        'PROGRESS': ('#D1826B', '#CC8019'), 
                                        'BORDER': 3, 'SLIDER_DEPTH': 5,  
                                        'PROGRESS_DEPTH': 5, } 
  
sg.theme('Tema')    
tela = [
        
        [sg.Image(r'D:\Ss1\Ss1_html_css\python\biju_meiri\logofundo.png', size=(550, 0))],

        [sg.Image(r'D:\Ss1\Ss1_html_css\python\biju_meiri\user.png'), sg.Input( key='Login', size=(45, 1))],

        [sg.Image(r'D:\Ss1\Ss1_html_css\python\biju_meiri\cadeado20.png'), sg.Input(key='senha', password_char=' ', size=(45, 1))],
        [sg.Checkbox('Salvar o login?', font='Arial 12', key='box')],
        [sg.Button('Entrar', size=(18, 1), font='Arial 12'), sg.Button('Esqueceu a senha?', size=(18, 1) ,font='Arial 12')]
]

layout = [[sg.Column(tela, element_justification='center')]]

window = sg.Window('Login', layout, grab_anywhere=True, size=(580,500))

while True:

    eventos, valores = window.read()

    if eventos == sg.WIN_CLOSED:
        break
    if eventos == 'Entrar':
        if valores['Login'] == '' and valores['senha'] == '':
                print(f'Adicione valores nos campos acima!')

        elif valores['Login'] == 'Admin' and valores['senha'] == 'Admin':

                print('Seja bem vindo!')

        else: 
                print('Login ou senha inválida!')

window.close()