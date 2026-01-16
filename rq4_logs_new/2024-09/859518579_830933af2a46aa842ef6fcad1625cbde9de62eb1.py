import pytesseract
import pyautogui
import keyboard as kb
import numpy as np
import pygetwindow as gw
import threading
import captcha
import time
import traceback
import json 


pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
# pyautogui.displayMousePosition()


class BotMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some
    possible methods include: base class, decorator, metaclass. We will use the
    metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class Binds(threading.Thread):
    def __init__ (self):
        threading.Thread.__init__(self)
        bot = Bot()
        kb.add_hotkey("1", bot.toggle)
        # kb.add_hotkey("2", bot.stop)
        # kb.add_hotkey("3", self.configurar)
        kb.add_hotkey("h", self.getMousePosition)
               
    def getConfigMousePosition(self):
        while True:
            try:
                if kb.is_pressed('space'):
                    pyautogui.sleep(2)
                    return pyautogui.position()
            except Exception as e:
                print("Except getConfigMousePosition ", e)

    def getMousePosition(self):
        print(pyautogui.position())
        return pyautogui.position()
    
    def run(self):
        kb.wait()

class Bot(metaclass=BotMeta):
    def __init__(self):
        self.resetConf()

    def click(self, x, y, tipo, button='left'):
        try:
            # if not captcha.is_clicando() and "otPokemon" in gw.getActiveWindow().title: #
                # pyautogui.moveTo(x,y,duration=0.2)#
                # pyautogui.sleep(0.2)
                # pyautogui.click(x, y,button=button,duration=0.2)#
                # pyautogui.sleep(0.2)
            self.lock.acquire()
            self.CLICKS.append([x,y, tipo, button])
            
            self.lock.release()
        except Exception as e:
            if not "'NoneType' object has no attribute 'title'" in str(e):
                print("Click exception ", e)
    
    def resetConf(self):

        # self.POS_LOOTS = [(956), 446)), (1038), 452))] # LOOTS 0acima de battle city
        # self.POS_LOOTS = [(1040, 525), (1040, 450)] # LOOTS SINGER
        # self.POS_LOOTS = [(1012), 448))] # hoenn elétrico ao lado de mauville
        self.loadConf()
        self.CLICKS = []
        self.TXT_POKES = ""
        self.TROCAR_POKES = False
        self.TROCAR_POKES_LUTA_DIFICIL = False
        self.BOT_ATIVADO = False
        self.SAFE_SLEEP = False
        self.FUGINDO = False
        self.PARAR_PESCA = False
        self.TIME_SAFE_POKE = 0
        self.threads = {}
        self.lock = threading.Lock()
        self.TIMER_PESCA = time.time() - 12
        self.TIMER_LOOT = time.time() - 12
        self.POKE_ATUAL = 0
        
    def _click(self):
        while self.BOT_ATIVADO:
            try:
                if "otPokemon" in gw.getActiveWindow().title:
                    clicks = self.CLICKS.copy()
                    
                    # 0 - TROCAR POKES
                    for click in clicks:
                        if click[2] == 'trocar_poke':
                            if not captcha.is_clicando():
                                pyautogui.moveTo(click[0],click[1],duration=0.2)
                                pyautogui.click(button=click[3])
                                self.CLICKS.remove(click)
                                clicks.remove(click)
                                pyautogui.sleep(0.2)
                                
                    # 1 - CAPTURAR
                    for click in clicks:
                        if click[2] == 'capturar':
                            if not captcha.is_clicando():
                                pyautogui.press("p")
                                pyautogui.moveTo(click[0],click[1],duration=0.3)
                                pyautogui.click(click[0],click[1],button=click[3])
                                self.CLICKS.remove(click)
                                clicks.remove(click)
                                pyautogui.sleep(0.2)
                           
                    # 2 - CADAVER     
                    for click in clicks:
                        if click[2] == 'cadaver':
                            if not captcha.is_clicando():
                                pyautogui.moveTo(click[0],click[1],duration=0.2)
                                pyautogui.click(button=click[3])
                            self.CLICKS.remove(click)
                            clicks.remove(click)
                            pyautogui.sleep(0.2)
                    
                    for click in clicks:
                        if not captcha.is_clicando():
                            if click[2] == 'batalhar':
                                if not self.isAtacando():
                                    pyautogui.moveTo(click[0],click[1],duration=0.2)
                                    pyautogui.click(button=click[3])
                                self.CLICKS.remove(click)
                                clicks.remove(click)
                                pyautogui.sleep(0.2)
                            elif click[2] == 'pescar':
                                pyautogui.moveTo(click[0],click[1],duration=0.2)
                                pyautogui.click(button=click[3])
                                self.CLICKS.remove(click)
                                clicks.remove(click)
                                pyautogui.sleep(0.2)
                            else:
                                print(click[2])
                                pyautogui.moveTo(click[0],click[1],duration=0.2)
                                pyautogui.click(button=click[3])
                                self.CLICKS.remove(click)
                                clicks.remove(click)
                                pyautogui.sleep(0.2)
            except Exception as e:
                if not "'NoneType' object has no attribute 'title'" in str(e):
                    print("Exception _click: ", e)

    def aguardarClick(self, click):
        while self.BOT_ATIVADO and click in self.CLICKS:
            pyautogui.sleep(0.1)
    
    def pegarLoot(self):
        ini = time.time()
        try:
            if "otPokemon" in gw.getActiveWindow().title:
                for loot in self.CONF["POS_LOOTS"]:
                    if captcha.is_captcha():
                        break
                    self.click(loot[0], loot[1], tipo='cadaver', button="right")
                    self.aguardarClick([loot[0], loot[1], 'cadaver', "right"])
                    pyautogui.sleep(0.3)
                    img = np.array(pyautogui.screenshot(region=self.CONF["RANGE_NOME_POKE_MORTO"]))
                    txt_cadaver =  pytesseract.image_to_string(img)
                    txt_cadaver = txt_cadaver.upper().replace('/[^a-zA-Z]/g', "")
                    if len(txt_cadaver.replace(' ', '')) > 3:
                        for i in range(5):
                            if pyautogui.pixelMatchesColor(self.CONF["POS_CLICK_ITEM_LOOT"][0], self.CONF["POS_CLICK_ITEM_LOOT"][1], (17,  27,  53), tolerance=8):
                                if pyautogui.pixelMatchesColor(self.CONF["POS_CLICK_ITEM_LOOT"][0]+5, self.CONF["POS_CLICK_ITEM_LOOT"][1]-2, (17,  27,  53), tolerance=8):
                                    break
                            self.click(self.CONF["POS_CLICK_ITEM_LOOT"][0], self.CONF["POS_CLICK_ITEM_LOOT"][1], tipo='loot', button="right")
                            self.aguardarClick([self.CONF["POS_CLICK_ITEM_LOOT"][0], self.CONF["POS_CLICK_ITEM_LOOT"][1], 'loot', "right"])
                        for cap in self.CONF["CAPTURAR"]:
                            if cap in txt_cadaver:
                                print("CAPTURAR ", txt_cadaver)
                                self.click(loot[0], loot[1], tipo='capturar')
                                pyautogui.sleep(0.2)
                                self.aguardarClick([loot[0], loot[1], 'capturar', "right"])
                                break
                fim = time.time()
                # print("Tempo pegarLoot: ", fim-ini)
        except Exception as e:
            print(e)
            return False

    def trocarPoke(self, pos):
        if pos != self.POKE_ATUAL and not self.FUGINDO:
            self.SAFE_SLEEP = True
            while True:
                self.click(self.CONF["POS_POKES"][pos][0], self.CONF["POS_POKES"][pos][1], tipo='trocar_poke')
                self.aguardarClick([self.CONF["POS_POKES"][pos][0], self.CONF["POS_POKES"][pos][1], 'trocar_poke', 'left'])
                pyautogui.sleep(1.5)
                if not self.meuPokeEstaMorto():
                    break
            self.SAFE_SLEEP = False
            self.POKE_ATUAL = pos

    def trocarPokes(self):
        for pos in range(len(self.CONF["POS_POKES"])):
            if not "otPokemon" in gw.getActiveWindow().title or not self.BOT_ATIVADO or not self.TROCAR_POKES:
                break
            while len(self.TXT_POKES) < 5:
                pyautogui.sleep(0.1)
            for i in range(1, 11):
                pyautogui.press(f'f{i}')
                pyautogui.sleep(0.02)
            pyautogui.sleep(1)
            self.trocarPoke(pos)
               
    def usarMoves(self):
        try:
            if "otPokemon" in gw.getActiveWindow().title:
                if self.TROCAR_POKES:
                    self.trocarPokes()
                elif self.CONF["CONF_NOME"] in self.CONF["MOVES"]:
                    for btn in self.CONF["MOVES"][self.CONF["CONF_NOME"]]:
                        if len(self.TXT_POKES) > 5:
                            pyautogui.press(btn)
                            pyautogui.sleep(0.05)
        except Exception as e:
            if not "'NoneType' object has no attribute 'title'" in str(e):
                print("Exception usarMoves: ",e)
                print(traceback.format_exc())

    def isAtacando(self):
        for x in range (self.CONF["POS_POKE_BATALHA"][0]-2, self.CONF["POS_POKE_BATALHA"][0]+4):
            cor = pyautogui.pixel(x, self.CONF["POS_POKE_BATALHA"][1])
            # if pyautogui.pixelMatchesColor(x, self.CONF["POS_POKE_BATALHA"][1], (255, 136, 136)) or pyautogui.pixelMatchesColor(x, self.CONF["POS_POKE_BATALHA"][1], (255, 0, 0)):
            if cor[0] > 240:
                return True
        return False
    
    def atacarPokes(self):
        while self.BOT_ATIVADO:
            ini = time.time()
            try:
                if "otPokemon" in gw.getActiveWindow().title:
                    # if time.time() - self.TIMER_LOOT > 3:
                    hp = self.getPokeHP()
                    if self.CONF["CONF_NOME"] in self.CONF["HEAL_KEYS"].keys():
                        for chave, valor in self.CONF["HEAL_KEYS"][self.CONF["CONF_NOME"]].items():
                            if valor >= hp:
                                pyautogui.press(chave)
                    captcha.procurar_captcha()
                    self.pegarLoot()
                        # self.TIMER_LOOT = time.time()
                    if not self.isAtacando():
                        # pyautogui.screenshot(imageFilename= 'range_batalha.png',region=self.CONF["RANGE_BATALHA"])
                        img = np.array(pyautogui.screenshot(region=self.CONF["RANGE_BATALHA"]))
                        self.TXT_POKES =  pytesseract.image_to_string(img)
                        if len(self.TXT_POKES) >= 5:
                            print("atacar", self.TXT_POKES)
                            self.click(self.CONF["POS_POKE_BATALHA"][0], self.CONF["POS_POKE_BATALHA"][1], tipo="batalhar")
                            self.usarMoves()
                            self.aguardarClick([self.CONF["POS_POKE_BATALHA"][0], self.CONF["POS_POKE_BATALHA"][1], "batalhar", "left"])
                        if (not self.TROCAR_POKES) and ("SHINY" in self.TXT_POKES.upper() or "HORDE" in self.TXT_POKES.upper() or "LEADER" in self.TXT_POKES.upper()): #in self.TXT_POKES.upper() or "HUNTAIL" in self.TXT_POKES.upper() or "TENTACRUEL" in self.TXT_POKES.upper() or "GOREBYSS" in self.TXT_POKES.upper() or "LANTURN" in self.TXT_POKES.upper()):
                            self.TROCAR_POKES = True
                            self.TROCAR_POKES_LUTA_DIFICIL = True
                            self.PARAR_PESCA = True
                            print("TROCAR_POKES_LUTA_DIFICIL ATIVADO")
                        elif self.TROCAR_POKES_LUTA_DIFICIL:
                            self.TROCAR_POKES = False
                            self.TROCAR_POKES_LUTA_DIFICIL = False
                            print("TROCAR_POKES_LUTA_DIFICIL DESATIVADO")
                            pyautogui.sleep(5)
                            self.PARAR_PESCA = False
                            self.trocarPoke(0)
                    
                    else:
                        self.usarMoves()
                    
                    fim = time.time()
                    # print("Tempo atacarPokes: ", fim-ini)
            except Exception as e:
                if not "'NoneType' object has no attribute 'title'" in str(e):
                    print("Erro no atacarPokes \n", e)
        
    def getPokeHP(self):
        iniTime = time.time()
        x1, x2, y = self.CONF["POS_POKE_LIFE"][0][0], self.CONF["POS_POKE_LIFE"][1][0], self.CONF["POS_POKE_LIFE"][0][1]
        dif = int(x2 - x1)
        res = 100
        ini = 0
        if not pyautogui.pixelMatchesColor(x1 + int(dif/2), y, (0,  17,  61)):
            ini = int(dif/2)
            
        for i in range(ini, dif, 3):
            if pyautogui.pixelMatchesColor(x1 + i, y, (0,  17,  61)):
                res = (i / dif) * 100
                break
        fim = time.time()
        # print("Tempo getPokeHP: ", fim-iniTime)
        return int(res)  

    def controlarPesca(self, hp=False):
        if not self.SAFE_SLEEP:
            if not hp:
                hp = self.getPokeHP()
            pokes = self.TXT_POKES.split('\n')
            for poke in pokes:
                for ignore in self.CONF["IGNORAR_POKES"]:
                    if ignore in poke.upper():
                        pokes.remove(poke)
            try:
                while True:
                    pokes.remove("")
            except ValueError:
                pass
            
            if self.pescaEstaAtivada():
                if hp <= 60 or len(pokes) >= 5 or captcha.is_captcha() or self.PARAR_PESCA:
                    self.desativarPesca()
            elif len(pokes) < 3:# and hp > 60:
                    self.ativarPesca()
        else:
            if self.pescaEstaAtivada() and captcha.is_captcha():
                    self.desativarPesca()

    def desativarPesca(self):
        if self.pescaEstaAtivada():
            print('desativarPesca')
            self.click(self.CONF["POS_PAUSE_PESCA"][0], self.CONF["POS_PAUSE_PESCA"][1], tipo='pescar')
            self.TIMER_PESCA = time.time()
            self.aguardarClick([self.CONF["POS_PAUSE_PESCA"][0], self.CONF["POS_PAUSE_PESCA"][1], 'pescar', 'left'])

    def ativarPesca(self):
        if captcha.is_captcha() or self.pescaEstaAtivada() or self.PARAR_PESCA:
            return
        
        if time.time() - self.TIMER_PESCA < 11:
            pyautogui.sleep(int(time.time() - self.TIMER_PESCA) + 1)
        if not self.pescaEstaAtivada():
            
            print('ativarPesca')
            self.click(self.CONF["POS_PAUSE_PESCA"][0], self.CONF["POS_PAUSE_PESCA"][1], tipo='pescar')
            self.aguardarClick([self.CONF["POS_PAUSE_PESCA"][0], self.CONF["POS_PAUSE_PESCA"][1], 'pescar', 'left'])

    def pescaEstaAtivada(self):
        # pyautogui.screenshot("botao_pesca.png", region=(self.CONF["POS_PAUSE_PESCA"][0]-15, self.CONF["POS_PAUSE_PESCA"][1]-15, 30, 30))
        try:
            box = pyautogui.locateOnScreen("./imgs/pause_pesca.png", confidence=0.85, region=(self.CONF["POS_PAUSE_PESCA"][0]-15, self.CONF["POS_PAUSE_PESCA"][1]-15, 30, 30))
            if box:
                return box
        except Exception as e:
            # print("pescaEstaAtivada", e)
            pass
        return False
    
    def useHeal(self):
        try:
            if "otPokemon" in gw.getActiveWindow().title:
                pyautogui.sleep(1)
                ini = time.time()
                hp = self.getPokeHP()
                if self.CONF["CONF_NOME"] in self.CONF["HEAL_KEYS"].keys():
                    for chave, valor in self.CONF["HEAL_KEYS"][self.CONF["CONF_NOME"]].items():
                        if valor >= hp:
                            pyautogui.press(chave)

                fim = time.time()
                # print("Tempo useHeal: ", fim-ini)
        except Exception as e:
            if not "'NoneType' object has no attribute 'title'" in str(e):
                print("Erro no useHeal \n", e)
         
    def meuPokeEstaMorto(self):
        return self.getPokeHP() <= 5
        
    def safe(self):
        while self.BOT_ATIVADO:
            if not self.SAFE_SLEEP:
                hp = self.getPokeHP()
                if self.detectarStaff() or hp < 5: #SE ENCONTRAR STAFF OU ESTIVER MORRENDO
                    self.fugir()
                else:
                    if self.CONF["CONF_NOME"] in self.CONF["HEAL_KEYS"].keys():
                        for chave, valor in self.CONF["HEAL_KEYS"][self.CONF["CONF_NOME"]].items():
                            if valor >= hp:
                                pyautogui.press(chave)
                                
                    self.controlarPesca(hp = hp)
                    if self.TIME_SAFE_POKE != 0:
                        if time.time() - self.TIME_SAFE_POKE > 20:
                            self.TROCAR_POKES = False
                            self.trocarPoke(0)
                            pyautogui.sleep(7)
                            self.PARAR_PESCA = False
                            self.TIME_SAFE_POKE = 0
                    elif not self.TROCAR_POKES:
                        if hp <= 30:
                            print("SAFE POKE - TROCAR DE POKEMON")
                            poke = 1
                            if self.POKE_ATUAL >= 5:
                                poke = 0
                            else:
                                poke = self.POKE_ATUAL+1
                            self.PARAR_PESCA = True
                            self.trocarPoke(poke)
                            self.TROCAR_POKES = True
                            self.TIME_SAFE_POKE = time.time()
                        
    def detectarStaff(self):
        tags = ['[Tutor]', '[Tutora]', '[Help]', '[CM]', '[GM]']
        if not self.BOT_ATIVADO:
            return
        try:
            if "otPokemon" in gw.getActiveWindow().title:
                img = np.array(pyautogui.screenshot(region=self.CONF["RANGE_CHAT"]))
                txt =  pytesseract.image_to_string(img)
                for tag in tags:
                    if tag in txt:
                        
                        print("!!STAFF DETECTADO!! ", tag)
                        print("_____________")
                        print(txt)
                        pyautogui.screenshot("staff.png")
                        return True
        except Exception as e:
            if not "'NoneType' object has no attribute 'title'" in str(e):
                print("Erro no detectarStaff \n", e)
            return False
        return False

    def fugir(self):
        print('fugir')
        self.FUGINDO = True
        self.click(self.CONF["POS_POKE_TP"][0], self.CONF["POS_POKE_TP"][1], tipo='poke_fugir')
        self.aguardarClick([self.CONF["POS_POKE_TP"][0], self.CONF["POS_POKE_TP"][1], 'poke_fugir', 'left'])
        self.stop()
        for i in range (10):
            pyautogui.press("0")
            pyautogui.sleep(1)
        pyautogui.keyDown("ctrl")
        pyautogui.sleep(0.2)
        pyautogui.press("l")
        pyautogui.sleep(0.2)
        pyautogui.keyUp("ctrl")
        pyautogui.sleep(0.2)
   
    def toggle(self):
        if self.BOT_ATIVADO:
            self.desativarPesca()
            self.stop()
            print("Desativado")
        else:
            self.start()
            print("Ativado")

    def start(self):
        self.stop()
        self.resetConf()
        self.BOT_ATIVADO = True
        self.threads = {}
        self.threads['_click'] = threading.Thread(target=self._click)
        self.threads['atacarPokes'] = threading.Thread(target=self.atacarPokes)
        self.threads['safe'] = threading.Thread(target=self.safe)
        # self.threads['controlarPesca'] = threading.Thread(target=self.controlarPesca)
        # self.threads['usarMoves'] = threading.Thread(target=self.usarMoves)
        # self.threads['procurar_captcha'] = threading.Thread(target=captcha.ligar_captcha)
        # self.threads['useHeal'] = threading.Thread(target=self.useHeal)
        for t in self.threads.copy():
            if t in self.threads and not self.threads[t].is_alive():
                self.threads[t].start()
    
    def stop(self):
        self.BOT_ATIVADO = False
        # for t in self.threads.copy():
        #     if t in self.threads and t != 'safe':
        #         print(f'{t} join')
        #         self.threads[t].join()
        #         print(f'{t} OK')
        #         self.threads.pop(t)

    def saveConf(self, conf = False):
        if not conf:
            conf ={ 
                "RANGE_BATALHA" : (1764, 579, 128, 210), 
                "RANGE_NOME_POKE_MORTO" : (1764, 972, 100, 18), 
                "RANGE_CHAT" : (201, 930, 150, 65), 
                "POS_LOOTS" : [(1040, 525), (1040, 450)],
                "POS_POKE_BATALHA": (1762, 591),
                "POS_CLICK_ITEM_LOOT": (1740, 1013),
                "POS_POKE_LIFE": [(60, 88), (155, 88)],
                "POS_POKE_TP": (38, 320),
                "POS_PAUSE_PESCA": (704, 118),
                "POS_POKES": [(34, 230), (34, 277), (34, 320), (34, 363), (34, 406), (34, 454)],
                "CONF_NOME": "togekiss",
                "HEAL_KEYS": {'sceptile': {} ,'togekiss': {'f6': 80, 'f10': 65, 'f5': 55}, 'padrao': {'f5': 60}},
                "MOVES": {'sceptile': ['f2', 'f4', 'f9', 'f1', 'f5', 'f6', 'f7', 'f8', 'f10'], 'togekiss': ['f9', 'f1', 'f3', 'f4', 'f8', 'f2', 'f7'], 'padrao': ['f1', 'f2', 'f3', 'f4', 'f6', 'f7', 'f8', 'f9', 'f10']},
                "CAPTURAR": ['Shiny', 'Horde', 'Leader', 'Tentacool', 'Clamper', 'Clanper', 'Clamperl', 'Clanperl', 'QUILFISH', 'QUILTISH', 'QWILFISH', 'QUWILFISH', 'POLIWHIRL','POLIUHIRL', 'POLIURATH', 'Polivrath', 'HUNTAIL', 'LANTURN', 'Wailmer', 'GOREBYSS', 'WHISCASH', 'WHISEASH', 'Tentacruel', 'Sharpedo', 'Crawdaunt', 'POLIWRATH', 'POLITOED', 'MANTINE'],
                "IGNORAR_POKES": ['Bellsprout', 'Spearow', 'Pidgey', 'Skiddo', 'Gogoat', 'Gloom'],
            } 
        
        for i, _ in enumerate(conf['CAPTURAR']):
            conf['CAPTURAR'][i] = conf['CAPTURAR'][i].upper()
        
        for i, _ in enumerate(conf["IGNORAR_POKES"]):
            conf["IGNORAR_POKES"][i] = conf["IGNORAR_POKES"][i].upper()
            #   self.CONF["IGNORAR"]
        # Convert and write JSON object to file
        with open("conf.json", "w") as outfile: 
            json.dump(conf, outfile)
            
    def loadConf(self):
        with open("conf.json", "r") as outfile: 
            self.CONF = json.load(outfile)

    def getConf(self):
        return self.CONF

def main():
    # while True:
    #     print(Bot().pescaEstaAtivada())
    Bot().toggle()
    binds = Binds()    
    binds.start()
    
if __name__ == "__main__":
    main()
    