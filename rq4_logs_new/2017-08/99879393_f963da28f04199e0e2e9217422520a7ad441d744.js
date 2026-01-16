'use strcit';

const net = require('net');
const log4js = require('log4js');
const receiver = new Map();

const logger = log4js.getLogger();
logger.level = 'debug';

let ID = 0;

const recvServer = net.createServer((sock) => {
  let id = ID++;
  logger.debug(id, 'receiver come in');
  sock.on('end', () => {
    receiver.delete(id);
    logger.debug(id, 'receiver client disconnected');
  });
  receiver.set(id, sock);
}).on('error', (err) => {
  logger.error(err);
}).listen(3000, () => {
  logger.info('recvServer bound');
});

const pushServer = net.createServer((sender) => {
  logger.debug('sender come in');
  sender.on('end', () => {
    logger.debug('send client disconnected');
  });
  for (let id of receiver.keys()) {
    let recv = receiver.get(id);
    logger.debug('sender ready to send');
    sender
      .pipe(recv)
      .on('error', (err) => {
        logger.error(err);
        sender.end();
        recv.end();
        receiver.delete(id);
      });
    break;
    // TODO support publish more recv
  }
}).on('error', (err) => {
  logger.error(err);
}).listen(3001, () => {
  logger.info('pushServer bound');
});