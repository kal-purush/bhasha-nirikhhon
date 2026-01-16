import atexit
import logging
import json
import time

from datetime import datetime, timedelta

from yahoo_finance import Share
from apscheduler.schedulers.background import BackgroundScheduler

from flask import Flask, request, jsonify

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

#logging initial setup
logging.basicConfig(format='%(asctime)-15s %(message)s')
logger = logging.getLogger('raw-data-producer')
logger.setLevel(logging.INFO)

#Assume your kafka service framework is deployed on the 192.168.99.100
kafka_broker = '192.168.99.100:9092'
topic_name = 'realtTime-StockAnalyzer'

#setup the kafka producer to put the raw data
producer = KafkaProducer(
    bootstrap_servers=kafka_broker
)

#schedule manage the multithreads
schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

#stock list record all the request predicted stock code
stocks = set()

#socket / thread shutdown , release occupation to the kafka server
def close_kafka_connection():
    try:
        #Flushing pending messages to kafka, timeout is set to 10s
        producer.flush(10)
    except KafkaError as kafka_error:
        logger.warn('kafka flush error, caused by: %s', kafka_error.message)
    finally:
        try:
            #Closing kafka connection
            producer.close(10)
        except Exception as e:
            logger.warn('fail to close kafka connection, caused by: %s', e.message)
    try:
        #shutdown threadpool
        schedule.shutdown()
    except Exception as e:
        logger.warn('fail to close threadpool, caused by: %s', e.message)

def grasp_stock_price(stockcode):
    logger.warn('grasp raw stock price of %s from yahoo', stockcode)
    try:
        year = timedelta(days=365)
        now_time = datetime.fromtimestamp(time.time())
        
        last_time = now_time - 1 * year
        stock_info = Share(stockcode)
        
        prices = stock_info.get_historical(last_time.strftime('%Y-%m-%d'), now_time.strftime('%Y-%m-%d'))

        for price in prices:
            tmp =[]
            tmp.append(price)
            single_price = json.dumps(tmp)
            producer.send(topic=topic_name, value=single_price, timestamp_ms=time.time())
        #Sent bunch of prices for stockcode to Kafka
    except KafkaTimeoutError as timeout_error:
        logger.warn('Fail to send stock %s price to kafka, caused by: %s', (stockcode, timeout_error.message))
    except Exception:
        logger.warn('Fail to grasp price for %s', stockcode)

app = Flask(__name__)
@app.route('/<stockcode>', methods=['POST'])
def create_stock_price_request(stockcode):
    if not stockcode:
        return jsonify({
            'error': 'empty stock'
        }), 400
    if stockcode not in stocks:
        #use schedule to manage the threads to increase new stock object
        schedule.add_job(grasp_stock_price, 'interval', [stockcode], seconds=10, id=stockcode)
        stocks.add(stockcode)
    return jsonify(list(stocks)), 200


@app.route('/<stockcode>', methods=['DELETE'])
def delete_stock_price_request(stockcode):
    if not stockcode:
        return jsonify({
            'error': 'empty stock'
        }), 400
    if stockcode in stocks:
        logger.warn("%s is deleted now",stockcode)
        schedule.remove_job(stockcode)
        stocks.remove(stockcode)
    return jsonify(list(stocks)), 200

if __name__ == '__main__':
    atexit.register(close_kafka_connection)
    #docker test is accept on localhost:5000, but the nodejs occupy on 3000
    app.run(host='0.0.0.0', port='5000')