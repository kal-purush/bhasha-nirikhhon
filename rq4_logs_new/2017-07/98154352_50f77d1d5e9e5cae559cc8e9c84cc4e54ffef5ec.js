import {MongoClient} from 'mongodb';
import assert from 'assert';
import MongoOplog from 'mongo-oplog'
const oplog = MongoOplog('mongodb://heroku_4dx26gc5:o0qqk68fgfa33un6memad2g80f@ds157702.mlab.com:57702/heroku_4dx26gc5?&authSource=admin', { ns: 'test.posts' })
// Connection URL
const url = 'mongodb://heroku_4dx26gc5:o0qqk68fgfa33un6memad2g80f@ds157702.mlab.com:57702/heroku_4dx26gc5';

export default function initial (callback) {

  MongoClient.connect(url, (err, db) => {
    assert.equal(null, err);
    console.log("Connected correctly to server");
    const collection = db.collection('Articles');
    oplog.tail().then(() => {
    console.log('tailing started')
  }).catch(err => console.error(err))
    oplog.on('update', doc => {
      console.log(doc);
    });
    // Find some documents
    collection.find({}).toArray((err, docs) => {
        assert.equal(err, null);
        console.log("Found the following records");
        callback(docs);
        db.close()
    });
  })
}