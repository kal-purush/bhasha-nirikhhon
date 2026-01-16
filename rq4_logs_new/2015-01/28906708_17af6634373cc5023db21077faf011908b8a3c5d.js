var mongoose = require('mongoose');

var dbURI = 'mongodb://localhost/recipeDirectory';
mongoose.connect(dbURI);
var db = mongoose.connection

// //outputs message to console, depending on connection status
db.on('error', console.error.bind(console, 'connection error: '));
db.once('connected', function() {
  console.log('Mongoose connected to ' + dbURI);
})

// //app database model
var recipesSchema = mongoose.Schema({
  name: String,
  cuisine: String,
  equipments: [String],
  ingredients: [String],
  method: [String]
});

mongoose.model('Recipe', recipesSchema);
// module.exports = Recipe;

// var newRecipe = new Recipe({
//   name: "Rice",
//   cuisine: "Chinese",
//   equipments: ["pot", "spoons", "sieve"],
//   ingredients: ["raw rice", "water", "salt"],
//   method: ["Boil water in a pot", "wash raw rice thoroughly", "add washed rice to boiling water and parboil", "sieve parboiled rice and add water and salt and boil till its done"]
// });

// var newRecipe = new Recipe({
//   name: "Salad",
//   cuisine: "American",
//   equipments: ["knife", "bowls", "salad spoons"],
//   ingredients: ["cabbage", "carrots", "green peas", "sweetcorn", "cucumber", "lettuce", "salad cream"],
//   method: ["Soak all vegetables for 5mins with water and vinegar", "grate carrots and cabbage and put in a bowl", "put other ingredients in the bowl", "mix with sald cream"]
// });

// var newRecipe = new Recipe({
//   name: "Beans",
//   cuisine: "Nigerian",
//   equipments: ["pressure pot", "knives", "frying pan"],
//   ingredients: ["raw beans", "water", "palm oil", "fresh pepper", "crayfish", "onions", "salt"],
//   method: ["Boil raw beans in pressure pot until very soft", "put frying pan on heat and add palm oil", "when palm oil is hot, add onions, fresh pepper, crayfish and salt", "add mixture to bean and stir", "taste for salt and serve hot"]
// });