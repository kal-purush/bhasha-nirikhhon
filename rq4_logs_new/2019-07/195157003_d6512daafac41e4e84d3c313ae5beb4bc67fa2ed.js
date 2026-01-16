const mongoose = require('mongoose')
const ingredientsSchema = new mongoose.Schema({
  id: Number,
  name: String
},
{ collection: 'ingredients' }
);

const recipeSchema = new mongoose.Schema({
    id: Number,
    name: String,
    cuisine: String,
    price: Number,
    ingredients: [ingredientsSchema]
  },
  { collection: 'recipe' }
);

const userSchema = new mongoose.Schema({
  username: String,
  password: String
},
{ collection: 'userdet' }
);


// you normally don't need to pass the name of the collection;
// the reason here is because otherwise it'll create a collection called pokemons
// we do not have a pokemons collection. So we specify the name of our collection here.


const recipe = mongoose.model('recipe', recipeSchema);
const ingredients = mongoose.model('ingridient', ingredientsSchema);
const userdet = mongoose.model('userdet', userSchema);

module.exports = {
  Recipe: recipe, Ingredients: ingredients, UserDet: userdet
}