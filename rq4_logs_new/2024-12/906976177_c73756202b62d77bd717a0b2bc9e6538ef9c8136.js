const express = require('express');
const app = express();
const cors = require('cors');

// Dotenv file required
require('dotenv').config();
const db = require('./database/db');

// Require API routes
const userAPI = require('./routes/user');
const bookAPI = require('./routes/book');
const favoritesAPI = require('./routes/favorites');
const cartAPI = require('./routes/cart');
const orderAPI = require('./routes/order');

// CORS
// app.use(cors());

app.use(cors({
    origin: 'http://localhost:5173', // Replace with your frontend URL
    credentials: true, // Allow credentials (cookies, HTTP authentication)
  }));


// DB Connection
db();



// tell the data is in JSON Format
app.use(express.json());

app.get('/', (req, res)=>{
    res.send('Hello World of Backend Devs')
});

// Routes or API
app.use('/api/v1', userAPI);
app.use('/api/v1', bookAPI);
app.use('/api/v1', favoritesAPI);
app.use('/api/v1', cartAPI);
app.use('/api/v1', orderAPI);

// listening Port
app.listen(process.env.PORT, ()=>{
    console.log(`Server on listening on PORT ${ process.env.PORT }`)
});