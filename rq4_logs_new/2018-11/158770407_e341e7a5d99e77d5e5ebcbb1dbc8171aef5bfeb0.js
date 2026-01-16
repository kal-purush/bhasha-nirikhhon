const MongoClient = require("mongodb").MongoClient;

const MONGO_URL = "mongodb://localhost:27017";

module.exports = app => {
  MongoClient.connect(MONGO_URL)
    .then(client => {
      console.log("Connection to mongo serve was established.");
      const db = client.db("koa-exp");
      app.people = db.collection("people");
      console.log("Reference to people collection was created.");
    })
    .catch(err => console.log(err));
};