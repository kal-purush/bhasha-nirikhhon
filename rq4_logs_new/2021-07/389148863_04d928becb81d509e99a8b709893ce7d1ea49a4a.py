import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains as A
import pandas as pd
from bs4 import BeautifulSoup
from telegram.ext import Updater, CommandHandler, ConversationHandler, MessageHandler, Filters
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
import requests
from selenium import webdriver
import sys
sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')
from selenium import webdriver

class vuelo():
  origin = ''
  destiny = ''
  number = ''
  fechaida = ''
  datervuelta = ''

  def start(update, context):
    name = update.message.from_user.name
    saludos = f"""
    Hola {name}! Esto es un bot de agencia de viaje!,\n/listarv para poder listar los viajes\n/comprar para comprar reservas de vuelos
    """
    update.message.reply_text(saludos)


  def origen(update, context):
      global origin
      origin = update.message.text.partition(' ')[2]
      print(origin)


  def destino(update, context):
      global destiny
      destiny = update.message.text.partition(' ')[2]
      print(destiny)


  def num_asiento(update, context):
      global number
      number = update.message.text.partition(' ')[2]
      print(number)

  def fecha_ida(update, context):
      global fechaida
      fechaida = update.message.text.partition(' ')[2]
      print(fechaida)


  def fecha_vuelta(update, context):
      global datervuelta
      datervuelta = update.message.text.partition(' ')[2]
      print(datervuelta)


  def listarv(update, context):
      name = update.message.from_user.name
      saludos = f"""
        Hola {name}! Para poder listar, debe seguir los siguientes pasos:\n1. /origen (ciudad, pais o codigo IATA) ejemplo: /origen quito\n2. /destino (ciudad, pais o codigo IATA) ejemplo: /destino guayaquil\n3. /listar para poder listar los vuelos
        """
      update.message.reply_text(saludos)

  def comprar(update, context):
      name = update.message.from_user.name
      comprar_saludos = f"""
            Hola {name}!\n/buy_ticket para reservar vuelos de ida\n/buyrt_ticket para reservar vuelos de ida y vuelta
            """
      update.message.reply_text(comprar_saludos)

  def buy_ticket(update, context):
      messager = f"""
            Para reserva el vuelo de ida, necesita ingresar los siguientes datos:\n1. /origen (ciudad, pais o codigo IATA) ejemplo: /origen quito\n2. /destino (ciudad, pais o codigo IATA) ejemplo: /destino guayaquil\n3. /num_asiento (Numero de asientos) ejemplo /num_asiento 3\n4. /fecha_ida (Fecha de partida) ejemplo /fecha_ida 16/03/2021\n5. /buy_vuelo una vez enviado todos los datos necesarios
            """
      update.message.reply_text(messager)


  def buyrt_ticket(update, context):
      messagert = f"""
            Para reserva el vuelo de ida y vuelta, necesita ingresar los siguientes datos:\n1. /origen (ciudad, pais o codigo IATA) ejemplo: /origen quito\n2. /destino (ciudad, pais o codigo IATA) ejemplo: /destino guayaquil\n3. /num_asiento (Numero de asientos) ejemplo /num_asiento 3\n4. /fecha_ida (Fecha de partida) ejemplo /fecha_ida 16/03/2021\n5. /fecha_vuelta (Fecha de retorno) ejemplo /fecha_ida 20/03/2021\n6. /buyrt_vuelo una vez enviado todos los datos necesarios
            """
      update.message.reply_text(messagert)


  def listar(update, context):
      global origin

      chrome_options = webdriver.ChromeOptions()
      chrome_options.add_argument('--headless')
      chrome_options.add_argument('--no-sandbox')
      chrome_options.add_argument('--disable-dev-shm-usage')
      wd = webdriver.Chrome('chromedriver',chrome_options=chrome_options)
      wd.get("https://www.trabber.ec/")

      a = A(wd)

      searchButton = wd.find_element_by_id("flight_type_one_ways")
      searchButton.click()

      search_field = wd.find_element_by_id("from_city_text")
      search_field.clear()
      search_field.send_keys(origin)
      time.sleep(1)
      a.double_click(search_field)

      search_field = wd.find_element_by_id("to_city_text")
      search_field.clear()
      search_field.send_keys(destiny)
      time.sleep(1)
      a.double_click(search_field)

      search_field = wd.find_element_by_id("from_date")
      search_field.clear()
      search_field.send_keys("21/03/2021")

      searchButton = wd.find_element_by_id("submit_button")
      searchButton.click()

      time.sleep(8)

      soup = BeautifulSoup(wd.page_source,'html.parser')

      table = soup.find("table", {'class': 'ResultsTable'})

      table_proccesed = []

      gg = soup.find_all('span', class_='AirportShort')[:14]

      nombre = list()
      probando = list()

      for i in gg:
          nombre.append(i.text)

      for i in range(0, len(nombre), 2):
          probando.append(nombre[i])

      # Salida
      gt = soup.find_all('td', align='right')[:14]

      nombre1 = list()
      probando1 = list()

      for i in gt[0:14]:
          nombre1.append(i.text)

      for i in range(0, len(nombre1), 2):
          probando1.append(nombre1[i])

      # Destino
      gy = soup.find_all('span', class_='AirportShort')[:14]

      nombre2 = list()
      probando2 = list()

      for i in gy:
          nombre2.append(i.text)

      for i in range(1, len(nombre2), 2):
          probando2.append(nombre2[i])


      # Llegada
      gu = soup.find_all('td', align='right')[:14]

      nombre3 = list()
      probando3 = list()

      for i in gt[0:14]:
          nombre3.append(i.text)

      for i in range(1, len(nombre3), 2):
          probando3.append(nombre3[i])

      # Aerolinea
      gi = soup.find_all('span', class_='AirlineLong')[1:8]

      nombre4 = list()
      for i in gi:
          nombre4.append(i.text)

      datos = {'Origen': probando, 'Salida': probando1, 'Llegada': probando3, 'Destino': probando2,
              'Aerolinea': nombre4}
      df = pd.DataFrame(
          {'Origen': probando, 'Salida': probando1, 'Llegada': probando3, 'Destino': probando2, 'Aerolinea': nombre4})
      df.to_csv('Gg.csv')

      wd.close()

      update.message.reply_text(f"{df}")


  def buy_vuelo(update, context):
      global origin, destiny, number, fechaida

      chrome_options = webdriver.ChromeOptions()
      chrome_options.add_argument('--headless')
      chrome_options.add_argument('--no-sandbox')
      chrome_options.add_argument('--disable-dev-shm-usage')
      wd = webdriver.Chrome('chromedriver',chrome_options=chrome_options)
      wd.get("https://www.trabber.ec/")

      a = A(wd)

      searchButton = wd.find_element_by_id("flight_type_one_ways")
      searchButton.click()

      search_field = wd.find_element_by_id("from_city_text")
      search_field.clear()
      search_field.send_keys(origin)
      time.sleep(1)
      a.double_click(search_field)

      search_field = wd.find_element_by_id("to_city_text")
      search_field.clear()
      search_field.send_keys(destiny)
      time.sleep(1)
      a.double_click(search_field)

      search_field = wd.find_element_by_id("from_date")
      search_field.clear()
      search_field.send_keys(fechaida)

      searchButton = wd.find_element_by_id("submit_button")
      searchButton.click()

      time.sleep(8)

      soup = BeautifulSoup(wd.page_source, 'html.parser')

      table = soup.find("table", {'class': 'ResultsTable'})

      table_proccesed = []

      gg = soup.find_all('span', class_='AirportShort')[:14]

      nombre = list()
      probando = list()

      for i in gg:
          nombre.append(i.text)

      for i in range(0, len(nombre), 2):
          probando.append(nombre[i])

      # Salida
      gt = soup.find_all('td', align='right')[:14]

      nombre1 = list()
      probando1 = list()

      for i in gt[0:14]:
          nombre1.append(i.text)

      for i in range(0, len(nombre1), 2):
          probando1.append(nombre1[i])

      # Destino
      gy = soup.find_all('span', class_='AirportShort')[:14]

      nombre2 = list()
      probando2 = list()

      for i in gy:
          nombre2.append(i.text)

      for i in range(1, len(nombre2), 2):
          probando2.append(nombre2[i])

      # Llegada
      gu = soup.find_all('td', align='right')[:14]

      nombre3 = list()
      probando3 = list()

      for i in gt[0:14]:
          nombre3.append(i.text)

      for i in range(1, len(nombre3), 2):
          probando3.append(nombre3[i])

      # Aerolinea
      gi = soup.find_all('span', class_='AirlineLong')[1:8]

      nombre4 = list()
      for i in gi:
          nombre4.append(i.text)

      #compra
      go = soup.find_all("td")

      nombre5 = list()
      probando5 = list()
      for i in go:
          if i.a != None:
              nombre5.append(i.a.get('href'))

      nombre6 = nombre5[3:34]


      for x in range(0, len(nombre6), 5):
          probando5.append(nombre6[x])
      print(probando5)
      #Precio
      gp = soup.find_all('a', class_='FlightPrice')[1:8]

      nombre7 = list()
      for i in gp:
          nombre7.append(i.text)

      datos = {'Origen': probando, 'Salida': probando1, 'Llegada': probando3, 'Destino': probando2,
              'Aerolinea': nombre4, 'Compra': probando5, 'Precio': nombre7}
      df = pd.DataFrame(
          {'Origen': probando, 'Salida': probando1, 'Llegada': probando3, 'Destino': probando2, 'Aerolinea': nombre4, 'Compra': probando5, 'Precio': nombre7})
      df.to_csv('Gg.csv')

      wd.close()

      for indice, fila in df.iterrows():
          update.message.reply_text(f"Sale de {fila['Origen']} a las {fila ['Salida']} y Llega a {fila['Destino']} a las {fila['Llegada']}\nPrecio: {fila['Precio']}")
          button1 = InlineKeyboardButton(
              text='Comprar',
              url='https://www.trabber.ec/'+f"{fila['Compra']}"
          )
          update.message.reply_text(
              text='Comprar ahora',
              reply_markup=InlineKeyboardMarkup([
                  [button1]
              ])
          )


  def buyrt_vuelo(update, context):
      global origin, destiny, number, fechaida, datervuelta

      chrome_options = webdriver.ChromeOptions()
      chrome_options.add_argument('--headless')
      chrome_options.add_argument('--no-sandbox')
      chrome_options.add_argument('--disable-dev-shm-usage')
      wd = webdriver.Chrome('chromedriver',chrome_options=chrome_options)
      wd.get("https://www.trabber.ec/")

      a = A(wd)

      search_field = wd.find_element_by_id("from_city_text")
      search_field.send_keys(origin)

      search_field = wd.find_element_by_id("to_city_text")
      search_field.clear()
      search_field.send_keys(destiny)

      search_field = wd.find_element_by_id("from_date")
      search_field.clear()
      search_field.send_keys(fechaida)

      search_field = wd.find_element_by_id("to_date")
      search_field.clear()
      search_field.send_keys(datervuelta)

      searchButton = wd.find_element_by_id("submit_button")
      searchButton.click()

      time.sleep(6)

      soup = BeautifulSoup(wd.page_source, 'html.parser')

      table = soup.find("table", {'class': 'ResultsTable'})

      table_proccesed = []

      for row in table.findAll("span")[5:]:
          row.proccesed = []
          cells = row.findAll("span")

      # ida
      gg = soup.find_all('span', class_='AirportShort')[:28]

      nombre = list()
      probando = list()

      for i in gg:
          nombre.append(i.text)

      for i in range(0, len(nombre), 4):
          probando.append(nombre[i])

      print(probando)
      print(len(probando))

      # Destino_ida
      gy = soup.find_all('span', class_='AirportShort')[:28]

      nombre2 = list()
      probando2 = list()

      for i in gy:
          nombre2.append(i.text)

      for i in range(2, len(nombre2), 4):
          probando2.append(nombre2[i])

      print(probando2)
      print(len(probando2))

      # regreso
      ggt = soup.find_all('span', class_='AirportShort')[:28]

      nombre3 = list()
      probando3 = list()

      for i in ggt:
          nombre3.append(i.text)

      for i in range(2, len(nombre3), 4):
          probando3.append(nombre3[i])

      print(probando3)
      print(len(probando3))

      # Destino_regreso
      ggtt = soup.find_all('span', class_='AirportShort')[:28]

      nombre4 = list()
      probando4 = list()

      for i in ggtt:
          nombre4.append(i.text)

      for i in range(0, len(nombre4), 4):
          probando4.append(nombre4[i])

      print(probando4)
      print(len(probando4))

      # precio
      gp = soup.find_all('a', class_='FlightPrice')[1:8]
      nombre7 = list()
      for i in gp:
          nombre7.append(i.text)

      print(nombre7)

      #compra
      go = soup.find_all("td")

      nombre5 = list()
      probando5 = list()
      for i in go:
          if i.a != None:
              nombre5.append(i.a.get('href'))

      nombre6 = nombre5[12:44]

      for x in range(0, len(nombre6), 5):
          probando5.append(nombre6[x])
      print(probando5)

      datos = {'Origenida': probando, 'Destinoida': probando2, 'Origenvuelta': probando3, 'Destinovuelta': probando4,
              'Precio': nombre7, 'Compra':probando5}
      df = pd.DataFrame(
          {'Origenida': probando, 'Destinoida': probando2, 'Origenvuelta': probando3, 'Destinovuelta': probando4,
          'Precio': nombre7, 'Compra':probando5})
      df.to_csv('Gg.csv')

      for indice, fila in df.iterrows():
          update.message.reply_text(f"Vuelo de ida:\nSale de {fila['Origenida']} y Llega a {fila['Destinoida']} la fecha {fechaida}\nVuelo de regreso:\nSale de {fila['Origenvuelta']} y Llega a {fila['Destinovuelta']} la fecha {datervuelta}\nPrecio: {fila['Precio']}")
          button1 = InlineKeyboardButton(
              text='Comprar',
              url='https://www.trabber.ec/'+f"{fila['Compra']}"
          )
          update.message.reply_text(
              text='Comprar ahora',
              reply_markup=InlineKeyboardMarkup([
                  [button1]
              ])
          )


  if __name__== '__main__':

      updater = Updater(token='1615424002:AAEWBM8wQeQD69EuJzWEsmeLsUBsNwgwuIw', use_context=True)

      dp = updater.dispatcher

      dp.add_handler(CommandHandler('start', start))
      dp.add_handler(CommandHandler('listarv', listarv))
      dp.add_handler(CommandHandler('origen', origen))
      dp.add_handler(CommandHandler('destino', destino))
      dp.add_handler(CommandHandler('num_asiento', num_asiento))
      dp.add_handler(CommandHandler('fecha_ida', fecha_ida))
      dp.add_handler(CommandHandler('fecha_vuelta', fecha_vuelta))
      dp.add_handler(CommandHandler('listar', listar))
      dp.add_handler(CommandHandler('comprar', comprar))
      dp.add_handler(CommandHandler('buy_ticket', buy_ticket))
      dp.add_handler(CommandHandler('buyrt_ticket', buyrt_ticket))
      dp.add_handler(CommandHandler('buy_vuelo', buy_vuelo))
      dp.add_handler(CommandHandler('buyrt_vuelo', buyrt_vuelo))

      updater.start_polling()
      updater.idle()