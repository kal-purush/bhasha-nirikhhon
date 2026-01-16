from confluent_kafka import Consumer, KafkaException
import matplotlib.pyplot as plt
import json

# Configuración del consumidor
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'crypto_group',
    'auto.offset.reset': 'earliest'
}

# Crear el consumidor
consumer = Consumer(conf)

# Suscribirse al tópico
consumer.subscribe(['crypto_price'])

#Sizes
btc_size = 0
eth_size = 0

#Capital trasacted
btc_capital = 0
eth_capital = 0

#Count mesagges receive
count_message = 0

# Consumir mensajes
try:
    while True:
        msg = consumer.poll(1.0)  # Espera por un mensaje durante 1 segundo
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        

        #Decodificar mensaje
        data = json.loads(msg.value().decode("utf-8"))

        #detectar y contar
        if data['product_id'] == 'BTC-USD':
            btc_size += data['last_size']
            btc_capital += data['transacted_capital']

        elif data['product_id'] == 'ETH-USD':
            eth_size += data['last_size']
            eth_capital += data['transacted_capital']


        count_message += 1


        if count_message == 200:
            
            print(" === BTC === ")
            print(f"Total de criptomonedas trasnferidas: {btc_size}")
            print(f"Total de capital trasnferido: {btc_capital}")

            print(" === ETH === ")
            print(f"Total de criptomonedas trasnferidas: {eth_size}")
            print(f"Total de capital trasnferido: {eth_capital}")

            #Reiniciamos contador
            count_message = 0


except KeyboardInterrupt:
    pass

finally:
    # Cerrar el consumidor
    consumer.close()