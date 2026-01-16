import { logger } from '../util/logger';
import { Room } from'../services/room/model';
import mongoose from 'mongoose';

const argv = process.argv.slice(2);
const objectId = argv[0];

if (!objectId) {
  logger.debug('Please provide ObjectIDs as command line arguments separated by space.');
  process.exit(0);
}

import { db } from './db';
db.then(() => {
    return Room.findByIdAndUpdate(objectId, {$set: {endTime: new Date()}}).exec();
  })
  .then((room : mongoose.Document) => {
    logger.debug(JSON.stringify(room, null, '\t'));
    process.exit(0);
  })
  .catch((err) => {
    logger.error(err);
    process.exit(0);
  });
