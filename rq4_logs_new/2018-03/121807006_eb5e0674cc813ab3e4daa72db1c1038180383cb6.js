// const MongoClient = require('mongodb').MongoClient;
const {MongoClient, ObjectID} = require('mongodb');

MongoClient.connect('mongodb://localhost:27017/TodoApp', (err, db) => {
    if (err) {
        return console.log('Unable to connect to MongoDB server');
    }
    console.log('Connected to MongoDB server');

    // db.collection('Todos').findOneAndUpdate({
    //     _id: new ObjectID('5a875c5917983c491a92fadf')
    // },{
    //     $set: {
    //         completed: true
    //     }
    // }, {  
    //     returnOriginal: false
    // }).then((result) => {
    //     console.log(result);
    // });

    db.collection('Users').findOneAndUpdate({
        _id: new ObjectID('5a8452328189e21486df9a7e')
    },{
        $inc: {
            age: 1
        },
        $set: {
            name: 'Gilo - Me'
        }
    }, {  
        returnOriginal: false
    }).then((result) => {
        console.log(result);
    });

    db.close();
});