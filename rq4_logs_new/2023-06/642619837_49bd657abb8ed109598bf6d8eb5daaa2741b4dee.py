from confluent_kafka import Producer
import json

# Configurações do Kafka
bootstrap_servers = 'localhost:9092'
topic = 'payments'

# Dados da mensagem
message = {
    "id": "1234567",
    "paymentStatus": "SUCCESSFUL"
}

# Serializa a mensagem para formato JSON
message_json = json.dumps(message)

# Função de callback para confirmação de entrega
def delivery_report(err, msg):
    if err is not None:
        print(f'Falha ao entregar a mensagem: {err}')
    else:
        print(f'Mensagem entregue com sucesso: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# Configura o produtor Kafka
conf = {
    'bootstrap.servers': bootstrap_servers
}
producer = Producer(conf)

# Produz a mensagem no tópico
producer.produce(topic, value=message_json, callback=delivery_report)

# Espera pela confirmação de entrega
producer.flush()