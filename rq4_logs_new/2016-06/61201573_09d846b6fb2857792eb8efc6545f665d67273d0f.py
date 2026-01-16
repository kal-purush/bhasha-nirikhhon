import logging
import pika
import time

LOGGER = logging.getLogger(__name__)


class RMQConsumer(object):
    """RabbitMQ consumer that will handle connection drop due to timeout
    or whatever reason, push these occurrence to metrics"""

    def __init__(self,
                 callback,
                 url,
                 port,
                 credentials,
                 exchange='',
                 exchange_type='fanout',
                 queue={},
                 routing_keys=[],
                 timeout=60
                 ):
        """
        Create a new instance of the consumer

        :param function callback: The function use to handle the messages
        it should return the result as a boolean value.
        :param str url: The RabbitMQ url to connect to
        :param integer port: The RabbitMQ port to connect through
        :param pika.PlainCredentials credentials: The AMQP credentials
        :param str exchange: The name of the exchange to consume
        :param str exchange_type: The type of the exchange to consume
        :param dict queue: Dict representing the queue
        :param list routing_keys: the routing keys to bind the queue to
        with the name of the queue, and the list of routing key to bind
        :param int timeout: The time in seconds to wait between two
        nack
        """
        self.connection = None
        self.channel = None
        self.closing = False
        self.consumer_tag = None
        self.callback = callback
        self.url = url
        self.port = port
        self.credentials = credentials
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.routing_keys = routing_keys
        self.timeout = timeout
        self.binding_left = 0

    def connect(self):
        """This method connects to RabbitMQ, returning the connection.
        When the connection is established, the on_connection_open method
        will be invoked by pika

        :rtype: pika.SelectConnection"""
        LOGGER.info('Connecting to %s', self.url)
        return pika.SelectConnection(
            pika.ConnectionParameters(
                credentials=self.credentials,
                host=self.url,
                port=self.port
            ),
            self.on_connection_open,
            stop_ioloop_on_close=False
        )

    def on_connection_open(self, _):
        """This method is called by pika once the connection to
        RabbitMQ has been established, It passes the handle to the
        connection object, but we do not use it"""
        LOGGER.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """This method is called whenever the connection is closed
        unexpectedly."""
        LOGGER.info('Adding connection close callback')
        self.connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ
        is closed unexpectedly. We will reconnect in this case

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given
        """
        self.channel = None
        if self.closing:
            self.connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self.connection.add_timeout(1, self.reconnect)

    def reconnect(self):
        """Will be invoked by the IOLopp timer if the connection is
        closed"""
        self.connection.ioloop.stop()
        if not self.closing:
            # create a new connection
            self.connection = self.connect()

            # There is now a new connection
            self.connection.ioloop.start()

    def open_channel(self):
        """Open a new channel with RabbitMQ, when RabbitMQ responds that
        the channel is open, the on_channel_open callback will be
        invoked"""
        LOGGER.info('Creating a new channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been
        opened. The channel object is passed in so we can make use of
        it"""
        LOGGER.info('Channel opened')
        self.channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.exchange)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel"""
        LOGGER.info('Adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """When the channel close we will close the connection"""
        LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self.connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange we will be consuming from

        :param str exchange_name: the name of the exchange"""
        LOGGER.info('Exchange declared')
        self.channel.exchange_declare(
            self.on_exchange_declareok,
            exchange_name,
            type=self.exchange_type,
            durable=True
        )

    def on_exchange_declareok(self, _):
        """Invoked when the exchange set up has finashed, is called
        with an unused frame"""
        LOGGER.info('Declaring queue %s', self.queue['name'])
        self.setup_queue(self.queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ

        :param dict queue_name: the name of the queue to declare"""
        self.channel.queue_declare(self.on_queue_declareok,
                                   self.queue['name'],
                                   self.queue.get('passive', False),
                                   self.queue.get('durable', False),
                                   self.queue.get('exclusive', False),
                                   self.queue.get('auto_delete', False),
                                   self.queue.get('nowait', False),
                                   self.queue.get('arguments', None))

    def on_queue_declareok(self, _):
        """"Method invoked when the queue declaration has completed
        that will bind every binding keys declared in the queue dict
        to the queue
        """
        self.binding_left = len(self.routing_keys)
        for routing_key in self.routing_keys:
            LOGGER.info('Binding %s to %s with %s',
                        self.exchange,
                        self.queue['name'],
                        routing_key)
            self.channel.queue_bind(self.on_bindok,
                                    self.queue['name'],
                                    self.exchange,
                                    routing_key)

    def on_bindok(self, _):
        """Method invoked when the binding of the queue and the routing
        key has completed"""
        self.binding_left -= 1
        LOGGER.info('Queue %d of %d bound',
                    len(self.routing_keys) - self.binding_left,
                    len(self.routing_keys))
        if self.binding_left == 0:
            self.start_consuming()

    def start_consuming(self):
        """This method first ensure that the consumer has a cancel
        callback and then start the consume"""
        LOGGER.info('Issuing consumer related commands')
        self.add_on_cancel_callback()
        self.consumer_tag = self.channel.basic_consume(self.on_message,
                                                       self.queue['name'])

    def add_on_cancel_callback(self):
        """Add a callback to permit to close cleanly the consumer"""
        LOGGER.info('Adding consumer cancellation callback')
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, _):
        """Invoked by pika when RabbitMQ send a cancel for a consumer"""
        LOGGER.info('Consumer was cancelled remotely, shutting down')
        if self.channel:
            self.channel.close()

    def on_message(self, channel, basic_deliver, properties, body):
        """Invoked when the consumer receive a message. The channel
        is passed, the basic_deliver holds the exchange, the routing key,
        the delivery tag and a redelivered flag. The body is the message
        that was sent. If the callback return true, it will ack the message
        if not, it will nack it.
        :param pika.channel.Channel channel
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode: body"""
        LOGGER.info('Received message # %s from %s',
                    basic_deliver.delivery_tag, properties.app_id)
        if self.callback(channel, basic_deliver, properties, body):
            LOGGER.info('Acknowledging message %s',
                        basic_deliver.delivery_tag)
            self.channel.basic_ack(
                delivery_tag=basic_deliver.delivery_tag)
        else:
            LOGGER.info('NACKing message %s and sleeping',
                        basic_deliver.delivery_tag)
            time.sleep(self.timeout)
            self.channel.basic_nack(
                delivery_tag=basic_deliver.delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by
        sending a cancel"""
        if self.channel:
            LOGGER.info('Sending Basic.Cancel command to RabbitMQ')
            self.channel.basic_cancel(self.on_cancelok, self.consumer_tag)

    def on_cancelok(self, _):
        """Invoked when RabbitMQ acknowledges the cancellation of a
        consumer. We will then close the channel, with the
        on_channel_closed method, that will close the connection"""
        LOGGER.info('Consumer cancellation acknowledged')
        self.close_channel()

    def close_channel(self):
        """Close the channel connection"""
        LOGGER.info('Closing the channel')
        self.channel.close()

    def run(self):
        """"Run the consumer by connecting to RabbitMQ and then start
        the IOLoop to block and allow the SelectConnection"""
        self.connection = self.connect()
        self.connection.ioloop.start()

    def stop(self):
        """"Cleanly close the connection to RabbitMQ by stopping
        the consumer with RabbitMQ. When RabbitMQ confirms the cancellation,
        on_cancelok will be invoked, which will close the channel and
        then the connection

        The IOLoop is started again because this method is invoked
        when CTRL-C is pressed, raising a KeyboardInterrupt exception.
        This exception stops the IOLoop which needs to be running for pika
        to communicate with RabbitMQ"""
        LOGGER.info('Stoping...')
        self.closing = True
        self.stop_consuming()
        self.connection.ioloop.start()
        LOGGER.info('Stopped')

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self.connection.close()
