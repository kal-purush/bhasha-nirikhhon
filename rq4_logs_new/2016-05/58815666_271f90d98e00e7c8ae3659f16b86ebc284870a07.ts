import * as fs from 'fs';
import * as cluster from 'cluster';

import Log from '../logger/model/logModel';
import { messageType } from './common';
const parentLogger = (filename: string, workers: Array<cluster.Worker>) => {
  const stream = fs.createWriteStream(filename);
  stream.write(Log.getHeader());
  const cluster = require('cluster');
  for (var id in workers) {
    workers[id].on("message", (data) => {
      if (data.type === 'sutyLog') {
        stream.write(data.log);
      }
    });
  }
}

export default parentLogger;