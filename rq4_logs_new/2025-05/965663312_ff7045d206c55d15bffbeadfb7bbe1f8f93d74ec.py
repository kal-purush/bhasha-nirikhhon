import json
import logging
import os
import pika
import requests
import sys
import threading
import time

# globals
logger = None

# constants
ORDER_SERVICE_PROT			= 5000
SHOPPING_CART_SERVICE_PORT	= 6000
USERS_SERVICE_PORT			= 7000
ITEMS_SERVICE_PORT			= 8000

def main():
	global logger
	
	logging.basicConfig(level=logging.INFO)
	logger = logging.getLogger()
	
	RabbitMqInit()
	
	ShoppingCartPolling()
	
	return

def ShoppingCartPolling():
	global logger
	
	url = f'http://sc_service:{SHOPPING_CART_SERVICE_PORT}/get_open_shopping_carts'
	while True:
		resp = requests.get(url=url)
		respJson = resp.json()
		
		for cartId, userId in respJson:
			logger.info(f'Open cart w/ id {cartId} for user {userId}, sending email...')
		
		sys.stdout.flush()
		
		time.sleep(1)
	
	return

###########################################################################
##	
##	RabbitMq initialization
##	
###########################################################################
def RabbitMqInit():
	helloWorldThread = threading.Thread(target=SetupRabbitMqHelloWorldConsumer, daemon=True)
	helloWorldThread.start()
	
	return

###########################################################################
##	
##	Setup RabbitMq hello world consumer
##	
###########################################################################
def SetupRabbitMqHelloWorldConsumer():
	global logger
	
	# read rabbitmq connection url from environment variable
	amqpUrl = os.environ['AMQP_URL']
	urlParams = pika.URLParameters(amqpUrl)
	
	# connect to rabbitmq
	connection = pika.BlockingConnection(urlParams)
	
	logger.info('Successfully connected to RabbitMQ')
	rmqChannel = connection.channel()
	
	# declare a new queue
	rmqChannel.exchange_declare(exchange='HelloWorldTesting', exchange_type='fanout')
	result = rmqChannel.queue_declare(queue='', exclusive=True)
	queueName = result.method.queue
	rmqChannel.queue_bind(exchange='HelloWorldTesting', queue=queueName)
	
	# setup consuming queues
	rmqChannel.basic_consume(queue=result.method.queue, on_message_callback=RmqHelloWorldCb, auto_ack=True)
	
	# start consuming
	rmqChannel.start_consuming()
	
	return

###########################################################################
##	
##	RabbitMq hello world consume callback
##	
###########################################################################
def RmqHelloWorldCb(channel, method, properties, body):
	global logger
	
	data = body.decode('utf-8')
	logger.info(f'RMQ: {data}')
	
	return

if __name__ == '__main__':
	main()