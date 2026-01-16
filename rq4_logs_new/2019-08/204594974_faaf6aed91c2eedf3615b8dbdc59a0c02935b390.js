let cheese_pizza_recipe = {
    "name": "cheese pizza",
    "ingredients": {
        "flour": 2,
        "tomatoes": 5,
        "cheese": 2
    },
    "preparation": {
        "dough": { "prepare": "pizza dough recipe" },
        "sauce": { "prepare": "tomato sauce recipe" },
        "cheese": { "action": "grate" }
    },
    "assembly": [
        { "operation": "rollOut",
          "argument": "dough"},
        { "operation": "spread",
        "argument": "sauce"},
        { "operation": "spread",
        "argument": "cheese"},
        { "operation": "bake",
        "argument": "10 min"}
    ]
};

let pepperoni_pizza_recipe = {
    "name": "pepperoni pizza",
    "ingredients": {
        "flour": 2,
        "tomatoes": 5,
        "cheese": 1,
        "pepperoni": 1
    },
    "preparation": {
        "dough": { "prepare": "pizza dough recipe" },
        "sauce": { "prepare": "tomato sauce recipe" },
        "cheese": { "action": "grate" },
        "pepperoni": { "action": "slice" }
    },
    "assembly": [
        { "operation": "rollOut",
          "argument": "dough"},
        { "operation": "spread",
        "argument": "sauce"},
        { "operation": "spread",
        "argument": "cheese"},
        { "operation": "spread",
        "argument": "pepperoni"},
        { "operation": "bake",
        "argument": "10 min"}
    ]
};

let mushroom_pizza_recipe = {
    "name": "mushroom pizza",
    "ingredients": {
        "flour": 2,
        "tomatoes": 5,
        "cheese": 1,
        "mushrooms": 7
    },
    "preparation": {
        "dough": { "prepare": "pizza dough recipe" },
        "sauce": { "prepare": "tomato sauce recipe" },
        "cheese": { "action": "grate" },
        "mushroom": { "action": "slice" }
    },
    "assembly": [
        { "operation": "rollOut",
          "argument": "dough"},
        { "operation": "spread",
        "argument": "sauce"},
        { "operation": "spread",
        "argument": "cheese"},
        { "operation": "spread",
        "argument": "mushroom"},
        { "operation": "bake",
        "argument": "10 min"}
    ]
};

let pizzaRecipes = {
    "cheese": cheese_pizza_recipe,
    "mushroom": mushroom_pizza_recipe,
    "pepperoni": pepperoni_pizza_recipe
};

let pizzaSalesEstimate = {
    "cheese": 20,
    "mushroom": 15,
    "pepperoni": 65
};

let prices = {
    "flour": 0.25,
    "tomatoes": 0.55,
    "cheese": 0.33,
    "pepperoni": 1.75,
    "mushrooms": 0.12
};

let ingredientsInPantry = {
    "flour": 10,
    "tomatoes": 10,
    "cheese": 8,
    "pepperoni": 3
};

function calculateIngredientListCost(ingredientList) {
    let cost = 0;
    let ingredients = Object.keys(ingredientList);
    for (let i = 0; i < ingredients.length; i++) {
        let ingredient = ingredients[i];
        let quantity = ingredientList[ingredient];
        let price = prices[ingredient];

        cost += quantity * price;
    }
    return cost;
}

function calculatePizzaCost(pizza) {
    let ingredientList = pizza.ingredients;
    return calculateIngredientListCost(ingredientList);
}

console.log(`${cheese_pizza_recipe.name} costs ${calculatePizzaCost (cheese_pizza_recipe)}`);
console.log(`${mushroom_pizza_recipe.name} costs ${calculatePizzaCost (mushroom_pizza_recipe)}`);
console.log(`${pepperoni_pizza_recipe.name} costs ${calculatePizzaCost (pepperoni_pizza_recipe)}`);

function addIngredients (ingredientList1, ingredientList2) {
    let newList = {};
    let ingredients = Object.keys(ingredientList1);
    for (let i = 0; i < ingredients.length; i++) {
        let ingredient = ingredients[i];
        let quantity = ingredientList1[ingredient];
        newList[ingredient] = quantity;
    }
    ingredients = Object.keys(ingredientList2);
    for (let i = 0; i < ingredients.length; i++) {
        let ingredient = ingredients[i];
        let quantity = ingredientList2[ingredient];
        if (newList[ingredient] != undefined) newList[ingredient] += quantity;
        else newList[ingredient] = quantity;
    }
    return newList;
}

function multiplyIngredients (ingredientList, multiplier) {
    let newList = {};
    let ingredients = Object.keys(ingredientList);
    for (let i = 0; i < ingredients.length; i++) {
        let ingredient = ingredients[i];
        let quantity = ingredientList[ingredient];
        newList[ingredient] = quantity * multiplier;
    }
    return newList;
}

function subtractIngredients (fromList, subList) {
    let newList = {};
    let ingredients = Object.keys(fromList);
    for (let i = 0; i < ingredients.length; i++) {
        let ingredient = ingredients[i];
        let quantity = fromList[ingredient];
        newList[ingredient] = quantity;
    }

    ingredients = Object.keys(subList);
    for (let i = 0; i < ingredients.length; i++) {
        let ingredient = ingredients[i];
        let quantity = subList[ingredient];
        if (newList[ingredient] != undefined) {
            if (quantity >= newList[ingredient]) delete newList[ingredient];
            else newList[ingredient] -= quantity;
        }
    }
    return newList;
}

function makeShoppingList(pizzaSalesEstimate, ingredientsInPantry) {
    let shoppingList = {};
    let pizzaTypes = Object.keys(pizzaSalesEstimate);
    for (let i = 0; i < pizzaTypes.length; i++) {
        let pizzaType = pizzaTypes[i];
        let ingredientList = pizzaRecipes[pizzaType].ingredients;
        shoppingList = addIngredients(shoppingList, multiplyIngredients(ingredientList, pizzaSalesEstimate[pizzaType]));
    }

    return shoppingList;
}

var addList = addIngredients({"pepperoni": 1, "cheese": 1}, {"cheese": 2});
console.log (addList);

var subList = subtractIngredients({"pepperoni" : 3, "cheese": 1, "tomatoes": 5}, {"pepperoni": 2, "cheese": 2, "flour": 2});
console.log(subList);

var multList = multiplyIngredients(pepperoni_pizza_recipe.ingredients, 10);
console.log(multList);

var shoppingList = makeShoppingList(pizzaSalesEstimate, {});
console.log(shoppingList);