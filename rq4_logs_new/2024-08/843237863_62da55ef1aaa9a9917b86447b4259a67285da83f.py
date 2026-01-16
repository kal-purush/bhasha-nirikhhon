import paho.mqtt.client as mqtt

# Configurações do RabbitMQ (Broker MQTT)
MQTT_HOST = "localhost"
MQTT_PORT = 1883  # Porta padrão para MQTT
MQTT_USER = "embarcados"
MQTT_PASSWORD = "embeddedSystems@2024"

# Função de callback quando a conexão é estabelecida
def on_connect(client, userdata, flags, rc):
    print(f"Conectado com código de resultado {rc}")
    # Publicar mensagem após conexão
    publish_message(client)

# Função para publicar uma mensagem
def publish_message(client):
    message = 10
    # Convertendo a mensagem para bytes (big-endian)
    message_bytes = message.to_bytes(2, byteorder="big")
    
    # Publicando a mensagem
    client.publish(
        topic="metrics.turbidity_metric",
        payload=message_bytes,
        qos=1  # Quality of Service level
    )
    print(f"Mensagem enviada: {message}")

# Criação do cliente MQTT
client = mqtt.Client()

# Configuração de credenciais
client.username_pw_set(username=MQTT_USER, password=MQTT_PASSWORD)

# Configuração de callbacks
client.on_connect = on_connect

# Conectar ao broker MQTT
client.connect(MQTT_HOST, MQTT_PORT, 60)

# Loop para manter a conexão
client.loop_forever()