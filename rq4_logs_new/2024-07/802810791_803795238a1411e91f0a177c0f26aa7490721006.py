import pika
import json

from models.products import Products


def update_product_stock(product_id, quantity):
    try:
        product = Products.objects.get(id=product_id, status=True, is_active=False)
        if product:
            if product.stock >= quantity:
                product.stock -= quantity
                product.save()
                print(f"Stock updated for product: {product_id}, new stock: {product.stock}")
            else:
                print(f"Not enough stock for product: {product_id}")
        else:
            print(f"Product not found: {product_id}")
    except Exception as e:
        print(f"Error updating product stock: {e}")


def callback(ch, method, properties, body):
    order_event = json.loads(body)
    product_id = order_event['product_id']
    quantity = order_event['quantity']

    # Update product stock
    update_product_stock(product_id, quantity)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_order_events():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='order_events', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='order_events', on_message_callback=callback)

    print("Started consuming order events...")
    channel.start_consuming()