######################################################################
# CABECALHO
# Adicione aqui as bibliotecas usadas para o desenvolvimento deste
# sistema. Dica: faca comentarios sem acentos. Isso evita erros de
# interpretacao do Python
import board
import adafruit_dht
import time
import glob, datetime

import Adafruit_SSD1306
from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont

######################################################################
# INICIALIZACAO
# Adicione aqui codigo para a inicializacao dos dispositivos do siste-
# ma embarcado. Normalmente, este codigo e encontrado no fabricante do
# dispositivo. Adicione, tambem constantes e variaveis do sistema.

###########################
# Inicializacao do OLED

# on the PiOLED this pin isnt used
RST = None
# Cria objeto oled com interface I2C
oled = Adafruit_SSD1306.SSD1306_128_32(rst=RST)
# Limpa display OLED e configura fonte
oled.begin()
oled.clear()
oled.display()
image = Image.new('1', (oled.width, oled.height))
draw = ImageDraw.Draw(image)
font = ImageFont.load_default()
top = -2

###########################
# Inicializacao do dht

#indica o pino GPIO da placa Raspberry onde a temperatura e
#a humidade devem ser lidas. Nota: em placas Raspberry Pi,
#use_pulseio=False pode ser necessario.
dhtDevice = adafruit_dht.DHT22(board.D4, use_pulseio=False)

###########################
# Inicializacao do pendrive
# Seleciona a interface USB onde os dados serão gravados
logging_folder = glob.glob('/media/pi/*')[0]
dt = datetime.datetime.now()
file_name = "temp_log_{:%Y_%m_%d}.csv".format(dt)
logging_file = logging_folder + '/' + file_name

######################################################################
# PROGRAMA PRINCIPAL
# Adicione aqui o codigo que captura os dados dos sensores. Faça as
# operações de escrita no display e pendrive. Este código deve perma-
# necer em execução - permanecer em loop - até ser abortado pelo usu-
# ário (ctrl+c).

intervalo_da_amostra = 2.0 # segundos
print("Escrevendo em : " + logging_file)

while True:
    ### Passo 1 - leia temperatura e humidade do sensor
    while True:
        try:
            # este codigo esta dentro de um try. Isso quer dizer que, caso
            # houver erro na leitura do sensor, o codigo sera desviado para
            # o tratamento de excecao (ver except)
            temperature_c = dhtDevice.temperature
            temperature_f = temperature_c * (9 / 5) + 32
            humidity = dhtDevice.humidity
            # registra momento da amostragem dos dados
            dt = datetime.datetime.now()
            print(
                "Temp: {:.1f} F / {:.1f} C    Humidity: {}% ".format(
                    temperature_f, temperature_c, humidity
                )
            )
            # se chegou ate aqui e porque nao houve erro na leitura do dht
            # quebra o while e vai para o passo 2
            break

        except RuntimeError as error:
            # o sensor DHT pode falhar ao ler.
            # Se isso ocorrer, repita a operacao de leitura
            print(error.args[0])
            time.sleep(2.0)
            continue
        
        except Exception as error:
            # erro critico. Sai do programa
            dhtDevice.exit()
            raise error

    ### Passo 2 - escreva temperatura e humidade no display 
    # Limpa imagem no display
    x = 0
    draw.rectangle((0,0,oled.width,oled.height), outline=0, fill=0)            
    # Escreve dados na memoria do display
    draw.text((x, top),       "Temperatura",  font=font, fill=255)
    draw.text((x, top+8),     "{:.1f} F / {:.1f} C".format(temperature_f,
                                                           temperature_c),
                                                           font=font, fill=255)
    draw.text((x, top+16),    "Umidade",  font=font, fill=255)
    draw.text((x, top+25),    "{}%".format(humidity),  font=font, fill=255)

    # Apresenta imagem
    oled.image(image)
    oled.display()

    ### Passo 3 - escreva temperatura e humidade no pendrive
    # Com os dados capturados do sensor, abre arquivo texto no pendrive
    # e escreve dados em formato csv (valores separados por virgula)
    f = open(logging_file, 'a')
    f.write("\n")
    f.write("{:%H:%M:%S}".format(dt) + ",")
    f.write(str(temperature_c) + "," + str(humidity))
    f.close()

    # intervalo entre amostras
    time.sleep(intervalo_da_amostra)