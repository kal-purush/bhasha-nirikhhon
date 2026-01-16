const mongoose = require('mongoose');
const mongoosePaginate = require("mongoose-paginate-v2");

const sizeSchema = new mongoose.Schema({
  size: { type: Number, required: false },
  stock: { type: Number, required: false },
});

const colorSchema = new mongoose.Schema({
    name: {type: String, required: false},
    color: {type: String, required: false},
})

const variationSchema = new mongoose.Schema({
  name: { type: String, required: false },
  color: {colorSchema},
  images: { type: [String], required: false },
  size: [sizeSchema],
  in_stock: { type: Boolean, required: false },
});

const shoeSchema = new mongoose.Schema({
  name: {type: String, required: true},
  brand: {type: String, required: true},
  gender: {type: String, required: true},
  category: {type: String, required: true},
  description: {type: String, required: true},
  in_stock: {type: Boolean, required: true},
  stock: {type: Number, required: false, default: null},
  images: {type: [String], required: false},
  in_discount: {type: Boolean, required: false},
  original_price: {type: Number, required: true},
  discount_price: {type: Number, required: false},
  have_variations: { type: Boolean, required: true },
  features: { type: [String], required: true },
  material: { type: String, required: true },
  color: {colorSchema},
  size: [sizeSchema],
  variations: [variationSchema],
  background_card: {type: String, required: true}
});


shoeSchema.plugin(mongoosePaginate)


const Shoe = mongoose.models.Shoe || mongoose.model('Shoe', shoeSchema);




export default Shoe