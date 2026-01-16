let categories = {
    "Salty": "SaltyFoods",
    "Sweet": "SweetFoods",
    "Spicy": "SpicyFoods",
    "Savory": "SavoryFods",
    "Sour": "SourFoods",
    "Baked": "BakedFoods",
    "Grilled": "GrilledFoods",

}

let frenchFries = {
    "name": "French Fries",
    "categories":["Salty", "Savory"],
    "isItPoss": true
    }

let cake = {
   "name": "Cake",
    "categories":["Sweet"],
   "isItPoss": true
}


let burger = {
   "name": "Burger",
    "categories":["Savory"],
    "isItPoss": true
}

let hotWings = {
    "name": "Hot Wings",
    "categories":["Spicy",],
    "isItPoss": true
}

let pasta = {
    "name": "Pasta",
    "categories":["Savory"],
    "isItPoss": true
}

let Lemons = {
    "name": "Lemons",
    "categories":["Sour"],
    "isItPoss": true
}

let sourCandy = {
    "name": "Sour Candy",
    "categories":["Sour"],
    "isItPoss": true
}

let salmon = {
    "name": "Salmon",
    "categories": ["Savory", "Baked"],
    "isItPoss": true
}

let grilledCheese = {
    "name": "Grilled Cheese",
    "categories": ["Grilled", "Savory"],
   " isItPoss": true
}

let Icecream = {
    "name": "Icecream",
    "categories": ["Sweet"],
   " isItPoss": true
}


let possibleFoods = [
    cake,
    frenchFries,
    burger,
    hotWings,
    pasta,
    Lemons,
    sourCandy,
    salmon,
    Icecream,
]


let lemonCategories  = Lemons["categories"];

console.log(lemonCategories)

function genFoodItem(){
    console.log("Button Was clicked");
    console.log("Button was clicked");
    console.log("you chose:")
    let radios = document.getElementsByName('q1');
    console.log(radios)  
    let userChoice;
    for (i = 0; i < radios.length; i++) {
        let radio = radios[i];
        if (radio.checked){
            userChoice = radio.value;
        }
    }

    console.log(`${userChoice}`);
    for (i = 0; i < possibleFoods.length; i++) {
        let food = possibleFoods[i];
        // console.log (food);
        let foodCategories = food["categories"];
        // console.log(foodCategories);
        // const foodCategories = ["JavaScript", "Python", "Ruby", "Java", "C++"];

        console.log(foodCategories.includes(userChoice))

        if (foodCategories.includes(userChoice)) {
            let list = document.querySelector("#generatedFoods")
            console.log(list);
            console.log(food["name"]);
            let newLi = document.createElement("li");
            newLi.textContent = `${food["name"]}`;
            list.appendChild(newLi)
        }
        
        





    }




}
