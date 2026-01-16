""" Automação utilizando programação orientada a objeto
no exemplo a seguir temos uma classe com metodos criados para: abrir algum programa especifico no pc
escrever, pesquisar, aguardar, clicar, extrair algum link e pegar a posiçao do mouse.
"""

import pyautogui
import pyperclip
import time


class Controller():

    def __init__(self):
        pyautogui.PAUSE = 1

    def abrir_programa(self, nome_programa):
        pyautogui.press("win")
        pyautogui.write(nome_programa)
        pyautogui.press("enter")

    def escrever(self, texto):
        pyperclip.copy(texto)
        pyautogui.hotkey("ctrl", "v")

    def escrever_e_enter(self, texto):
        self.escrever(texto)
        pyautogui.press("enter")

    def entrar_site(self, site, espera=3):
        self.escrever_e_enter(site)
        self.aguardar(espera)

    def aguardar(self, tempo=3):
        time.sleep(tempo)

    def clicar(self, pos_x, pos_y, botao="left"): #por padrao=esquerda, para clicar com o botao direito, necessita passar o parametro botao='right'
        pyautogui.click(pos_x, pos_y, button=botao)

    def pegar_posicao(self):     # registra a posição do mouse
        for i in range(5):
            print(f"pegando posicao em {5 - i} segundos")
            time.sleep(1)
        print(pyautogui.position())

    def extrair_link(self, pos_x, pos_y, posicao_link_menu=2):
        self.clicar(pos_x, pos_y, botao="right")
        for i in range(posicao_link_menu):
            pyautogui.press("up")
        pyautogui.press("enter")
        texto = pyperclip.paste()
        print(texto)



if __name__ == "__main__":

    controlador = Controller()
    controlador.abrir_programa("chrome")
    controlador.entrar_site("https://www.youtube.com/")
    controlador.clicar(x=, y=)
    controlador.escrever_e_enter("python")
    controlador.aguardar()
    controlador.clicar(x=, y=)
    controlador.aguardar()
    controlador.extrair_link(x=, y=)


    controlador.pegar_posicao()   # pega a posição do mouse x , y




