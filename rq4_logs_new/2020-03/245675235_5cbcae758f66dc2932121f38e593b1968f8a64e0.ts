import { Db, MongoClient } from 'mongodb';
import { accountIndexes } from '@app/models/account';
import { codeIndexes } from '@app/models/code';
import { historyIndexes } from '@app/models/history';
import { pendingActionIndexes } from '@app/models/pendingAction';
import * as _ from 'lodash';

let client: MongoClient;
let callboxDb: Db;

const collectionIndexes = [
  accountIndexes,
  codeIndexes,
  historyIndexes,
  pendingActionIndexes
];

export async function connect(): Promise<void> {
  try {
    client = await MongoClient.connect(process.env.MONGODB, { useUnifiedTopology: true });
    callboxDb = client.db('callbox');

    await Promise.all(
      _.flatMap(collectionIndexes, indexes => indexes.map(i => db().collection(i.collection).createIndex(i.index, i.options))),
    );
  } catch(e) {
    console.log(e);
  }
}

export const db = () => callboxDb;
export const codes = () => db().collection('codes');
export const accounts = () => db().collection('accounts');
export const history = () => db().collection('history');

export type Index = {
  collection: string,
  index: string | any,
  options?: any
};