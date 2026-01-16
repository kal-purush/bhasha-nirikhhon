import express from 'express'
import DataStore from 'nedb'

const PORT = 8000

// BDD
const db = new DataStore({ filename: "perso" })
db.loadDatabase()

// demarrage d'express
const app = express()
app.use(express.json())

// API CRUD

// Create
app.post('/api/perso', (req, res) => {
    console.log(req.body)
    db.insert(req.body)
    res.send(req.body)
})

// Read ALL
app.get('/api/perso', (req, res) => {
    db.find({}, (err, docs) => {
        if (err) console.log(err)

        res.send(docs)
    })
})

// Read ONE
app.get('/api/perso/:id', (req, res) => {
    db.find({ _id: req.params.id }, (err, docs) => {
        if (err) console.log(err)

        res.send(docs)
    })
})

// Update
app.patch('/api/perso/:id', (req, res) => {
    db.update({ _id: req.params.id }, { $set: { ...req.body } })
    res.send(req.body)
})

// Delete
app.delete('/api/perso/:id', (req, res) => {
    db.remove({ _id: req.params.id }, (err, docs) => {
        if (err) console.log(err)

        res.send(docs)
    })
})







app.listen(PORT, () => {
    console.log(`Serveur lancé sur le port ${PORT}`)
})