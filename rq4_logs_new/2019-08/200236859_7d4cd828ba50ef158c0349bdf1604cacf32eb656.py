from flask import Flask, redirect, url_for, jsonify, request, render_template
import pika

app = Flask(__name__)
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')
channel.basic_publish(exchange='',routing_key='hello',body='Hello World!')

print("[x] Sent: 'Hello World!'")

connection.close()

@app.route('/hello')
def greeting():
    return 'hello'

if __name__ == '__main__':
    app.run(debug=True)