const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const session = require('express-session');
const massive = require('massive');

const connectionString = 'postgres://postgres:Prince11j9@localhost/sandbox';

const massiveConnection = massive.connectSync( {
  connectionString: connectionString
})

const app = express();
const port = 3000;


app.set('db', massiveConnection)
const db = app.get('db');

app.use(bodyParser.json());
app.use(cors());
app.use(session({secret: 'keyboard cat'}))



app.get('/api/:id', function(req, res) {
  db.read_product(req.params.id, function(err, product) {
    if (err) return res.status(500).json(err)
    return res.status(200).json(product)
  })
})

app.get('/api', function(req, res) {
  db.read_products(function(err, products) {
    if (err) return res.status(500).json(err)
    return res.status(200).json(products)
  })
})

app.post('/api', function(req, res) {
  db.create_products(function(err, createProduct) {
    if (err) return res.status(500).json(err)
    return res.status(200).json(createProduct)
  })
})

app.put('/api', function(req, res) {
  db.update_product(req.body.description, req.body.id, function(err, updateProduct) {
    if (err) return res.status(500).json(err)
    return res.status(200).json(updateProduct)
  })
})

app.delete('/api/:id', function(req, res) {
  db.delete_product(req.params.id, function(err, deleteProduct) {
    if (err) return res.status(500).json(err)
    return res.status(200).json(deleteProduct)
  })
})



app.listen(port, () => {
  console.log(`listening on port ${port}`)
})