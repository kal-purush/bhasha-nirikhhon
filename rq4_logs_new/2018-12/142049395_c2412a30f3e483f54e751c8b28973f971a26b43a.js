const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const dbName = 'kodflix';
const url = 'mongodb://localhost:27017';

module.exports.mongo = {
    client: null,
    connectAndGetCollection: function(){
        return new Promise((resolve, reject) => {
            MongoClient.connect(url, (err, client) => {
                let db = client.db(dbName);
                let collection = db.collection('shows');
                this.client = client;
                if(err) {
                   reject(err, client);
               }
               else {
                   resolve(collection);
               }
            });
        });
    },
    getTheData : function(collection, id = null){
        let qry = id !== 'all' ? {id: id} : {};
        return new Promise((resolve, reject) => {
            collection.find(qry).toArray((err, docs) => {
                this.client.close();
                if(err){
                    reject(err, this.client);
                }
                else{
                    resolve(id !== 'all' ? docs[0] : docs);
                }
              }); 
        });
    }
}