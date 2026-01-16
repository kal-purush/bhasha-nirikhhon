// CRUD // create, read, update, delete

const { MongoClient, ObjectID } = require('mongodb');

const connectionURL = 'mongodb://127.0.0.1:27017';
const databaseName = 'task-manager-app';

MongoClient.connect(connectionURL, { useNewUrlParser: true }, (error, client) => {
  if (error) {
    return console.log('Unable to connect to database');
  }
  const db = client.db(databaseName);

  db.collection('tasks').deleteOne({
    _id: new ObjectID('5cf43dd5e785371face9c877')
  }).then((result) => {
    console.log(result.deletedCount)
  }).catch((error) => {
    console.log(error);
  });

  // db.collection('users').deleteMany({
  //   age: 27
  // }).then((result) => {
  //   console.log(result.deletedCount)
  // }).catch((error) => {
  //   console.log(error)
  // });

  // db.collection('tasks').updateMany({
  //   completed: false
  // },{
  //   $set: {
  //     completed: true
  //   }
  // }).then((result) => {
  //   console.log(result.modifiedCount)
  // }).catch((error) => {
  //   console.log(error)
  // });

  // using callback

  // db.collection('tasks').findOne({ _id: new ObjectID("5cf43dd5e785371face9c879") }, (error, task) => {
  //   if (error) {
  //     return console.log('Unable to find task');
  //   }
  //   console.log(task);
  // });
});