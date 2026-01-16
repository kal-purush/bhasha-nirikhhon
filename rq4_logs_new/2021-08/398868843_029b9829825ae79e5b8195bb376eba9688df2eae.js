const express = require('express');
const path = require('path');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;

// >>> local
const MONGO_URL = 'mongodb://admin:password@localhost:27017'
// >>> container running in the same Docker network as 'mongodb' service in `mongo.yaml`
// same network so a lot easier to connect
// const MONGO_URL = 'mongodb://admin:password@mongodb'

const server = express()

server.use(express.static('images'))

const bodyParser = require('body-parser')
server.use(bodyParser.urlencoded({ extended: true }))
server.use(bodyParser.json())

server.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, "index.html"))

    // res.json({
    //     text: "Hello world "
    // })
})

server.get('/lenny', (req, res) => {
    res.json({
        text: "Hello lenny "
    })
})

server.get('/profile/photo', (req, res) => {
    console.log(path.join(__dirname, "images/profile.png"))
    const img = fs.readFileSync(path.join(__dirname, "images/profile.png"))
    res.writeHead(200, { 
        'Content-Type': 'image/png'
    })
    res.end(img, 'binary')
})

server.get('/get-profile', (req, res) => {
    // username and password set 
    MongoClient.connect(MONGO_URL, (err, client) => {
        if (err) throw err

        const db = client.db('user-account')
        const query = { userid: 1 }
        db.collection('users').findOne(query, (err, dbResult) => {
            if (err) throw err
            client.close()
            res.json(dbResult)
        })
    })
})

server.post('/update-profile', (req, res) => {
    const userObj = req.body

    console.log("Connecting to db...")

    MongoClient.connect(MONGO_URL, (err, client) => {
        if (err) throw err
        console.log("Succesfully connected to user-account db...")

        const db = client.db('user-account')
        userObj['userid'] = 1
        const query = { userid: 1 }
        
        db.collection('users').updateOne(query,
            { $set: userObj },
            { upsert: true },
             (err, dbResult) => {
                if (err) throw err
                console.log("Successfully updated/inserted")
                client.close()
                res.json(userObj)
        })
    })
})

server.listen('3000', () => {
    console.log("listening")
})