const MongoClient = require('mongodb').MongoClient;
const mongodb = require('mongodb');

const url = "mongodb://localhost:27017/";

// async function getAccountsByIds(ids) {

//   const obj_ids = ids.map(function(id) { return mongodb.ObjectId(id); });

//   const accounts = new Promise((resolve, reject) => {
//     MongoClient.connect(url, function(err, db) {
//       if (err) reject(err);
//       const budgeterDb = db.db('budgeter');
//       budgeterDb.collection("accounts").find({_id: {$in: obj_ids}}).toArray(function(err, accounts){
//         if (err) reject(err);
//         resolve(accounts);
//       });
//     });
//   });

//   return accounts;

// }

module.exports = {
  MongoClient,
  url,
  // getAccountsByIds
};