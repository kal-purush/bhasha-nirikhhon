import connectToDb from 'jscommons/dist/mongoRepo/utils/connectToDb';
import { Db } from 'mongodb';
import { once } from 'lodash';
import config from '../../config';
import logger from '../../logger';

export default once((): () => Promise<Db> => {
  return connectToDb({
    dbName: config.mongo.dbName,
    logger,
    url: config.mongo.url,
  });
});