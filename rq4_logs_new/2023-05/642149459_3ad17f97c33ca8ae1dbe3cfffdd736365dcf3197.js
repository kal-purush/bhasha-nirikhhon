const Recipe = require('../models/recipe')

const getAllRecipe = (req,res,next) => {
    Recipe.fetchAllRecipes((recipeData, err) => {
        if(err){
            //this is an error
            res.render('error', { title: "Something went wrong", message: err.message })
        }
        res.render('recipes', { recipes: recipeData, title: "Recipe List" })
    })
}

const getAddRecipePage = (req,res,next) => {
    res.render('create', { title: "New Recipe" })
}

const postAddRecipe = (req,res,next) => {
    console.log(req.body);
    let {name, ingredient, instruction, quantity} = req.body

    if(!Array.isArray(ingredient)){
        ingredient = [ingredient]
        quantity = [quantity]
    }

    if(!Array.isArray(instruction)){
        instruction = [instruction]
    }

    const ingredients = ingredient.map((ing, i) => {
        //ingredient ---> ['flour', 'sugar', 'butter']
        //quantity ---> ['1 cup', '4 cups', '1 bar']
        return { name: ing, quantity: quantity[i] }
    })

    const newRecipe = new Recipe(name, ingredients, instruction)
    newRecipe.save(({ message, status }) => {
        if(status === 201){
            return res.redirect('/recipes')
        }

        res.status(status).render('error', { title: "Something went wrong", message })
    })
}

module.exports = {
    getAllRecipe,
    getAddRecipePage,
    postAddRecipe
}