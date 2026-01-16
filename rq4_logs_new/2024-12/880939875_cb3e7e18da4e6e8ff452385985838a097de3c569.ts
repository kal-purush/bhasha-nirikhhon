import Bree from 'bree';
import Graceful from '@ladjs/graceful';
import logger from '@/logging';
import path from 'path';

const bree = new Bree({
  root: path.join(process.cwd(), 'dist', 'tasks'),
  jobs: [
    {
      name: 'ocv-vote-counting',
      path: path.join(process.cwd(), 'dist', 'tasks', 'ocv-vote-counting.js'),
      interval: '1m',
      timeout: 0, // start immediatly when this script is run
    }
  ]
});

const graceful = new Graceful({ brees: [bree] });
graceful.listen();

(async () => {
  await bree.start();
  logger.info('Bree started successfully');
})();