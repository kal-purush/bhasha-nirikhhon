const jsonfile = require('jsonfile');
const FILE = 'data.json';

const express = require('express');
const app = express();

app.use(express.static(__dirname+'/public/'));
app.use(express.json());
app.use(express.urlencoded({
  extended: true
}));

const methodOverride = require('method-override')
app.use(methodOverride('_method'));

const reactEngine = require('express-react-views').createEngine();
app.engine('jsx', reactEngine);
app.set('views', __dirname + '/views');
app.set('view engine', 'jsx');

/////GENERATES FORM TO ADD NEW RECIPE//////
app.get('/recipes/new', (request, response) => {
    response.render('New');
});

/////SHOWS NEWLY ADDED RECIPE AFTER FORM SUBMISSION//////
app.post('/recipes', (request, response) => {
    let newRecipe = request.body;

    const data = {
        "id": newRecipe.id,
        "title": newRecipe.title,
        "ingredients": newRecipe.ingredients,
        "instructions": newRecipe.instructions
    };

    jsonfile.readFile(FILE, (err, obj) => {
        obj.recipes.push(newRecipe);

    jsonfile.writeFile('data.json', obj, (err) => {
        console.error(err)

        response.render('Recipes', data);
    });
    });
});

/////SHOWS INDIVIDUAL RECIPE PAGE////
app.get('/recipes/:id', (request, response) => {

    jsonfile.readFile(FILE, (err, obj) => {
        let inputId = parseInt(request.params.id);
        var recipe;
        let recipeList = obj.recipes;

        //look inside data.json for the recipe
        recipeList.forEach(function(recipeItem){
            console.log(recipeItem);
            let selectedRecipe = recipeItem;

            if (selectedRecipe.id === inputId){
                recipe = selectedRecipe;
                const data = {
                    id: selectedRecipe.id,
                    title: selectedRecipe.title,
                    ingredients: selectedRecipe.ingredients,
                    instructions: selectedRecipe.instructions
                };
            response.render('Recipes', data);
            };
        });

        if (recipe === undefined && request.params.id!== "new") {
            // send 404 back
            response.status(404);
            response.send("This recipe does not exist");
        };
    });
});


/**
 * ===================================
 * Listen to requests on port 3000
 * ===================================
 */
app.listen(8000, () => console.log('~~~ Tuning in to the waves of port 8000 ~~~'));