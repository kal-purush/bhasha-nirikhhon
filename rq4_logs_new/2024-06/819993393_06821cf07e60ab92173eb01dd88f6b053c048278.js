// firebase.js
var admin = require("firebase-admin");

var serviceAccount = require("ai-writing-busy-readers-firebase-key.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

module.exports = db;