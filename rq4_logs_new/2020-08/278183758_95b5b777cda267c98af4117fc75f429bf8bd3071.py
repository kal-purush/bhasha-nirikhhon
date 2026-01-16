import asyncio
import aio_pika
import aiosmtplib
from email.message import EmailMessage


async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        email_message = EmailMessage()
        email_message["From"] = "your mail"
        email_message["To"] = "mail to receive"
        email_message["Subject"] = "Hi everyone"
        email_message.set_content(message.body.decode("utf-8"))

        await aiosmtplib.send(email_message, hostname="mail", port=25)
        await asyncio.sleep(1)


async def listener(loop):
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@rabbit/", loop=loop, port=5672,
    )

    queue_name = "messages"


    # Creating channel
    channel = await connection.channel()

    # Maximum message count which will be
    # processing at the same time.
    await channel.set_qos(prefetch_count=100)

    # Declaring queue
    queue = await channel.declare_queue(queue_name)

    await queue.consume(process_message)