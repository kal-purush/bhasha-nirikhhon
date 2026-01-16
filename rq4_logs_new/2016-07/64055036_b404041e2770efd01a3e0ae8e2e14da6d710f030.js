const amqp = require('amqplib/callback_api')
const Config = require('../config')

amqp.connect(Config.amqp, (err, conn) => {
  if (err) {
    console.error('Unable to connect: ', err)
    process.exit(1)
  }

  conn.createChannel((err, ch) => {
    if (err) {
      console.error('Channel error: ', err)
      process.exit(1)
    }

    ch.assertExchange(Config.channel, 'fanout', { durable: false })
    ch.assertQueue('', { exclusive: true }, (err, q) => {
      if (err) {
        console.error('Queue exception: ', err)
        process.exit(1)
      }

      console.log(' [*] Waiting for messages in %s. To exit press CTRL+C', q.queue)

      ch.bindQueue(q.queue, Config.channel, '')
      ch.consume(q.queue, function (msg) {
        let json = JSON.parse(msg.content.toString())
        console.log(json)
      }, { noAck: true })
    })
  })
})