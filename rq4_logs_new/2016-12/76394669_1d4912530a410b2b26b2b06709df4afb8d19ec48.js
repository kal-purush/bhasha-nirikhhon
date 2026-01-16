/*  Control the behavior of our to do products app  */

// Our requires
var bodyParser = require('body-parser');
var mongoose = require('mongoose');

// Connect to the database
mongoose.connect('mongodb://localhost:27017/products');

// Create a schema like a blueprint for our data
var productsSchema = new mongoose.Schema({
    brand: String,
    model: String,
    megapixels: [Number],
    rating: [Number],
    price: [Number]
});

// Way to connect to mongoose
var Products = mongoose.model('Products', productsSchema);