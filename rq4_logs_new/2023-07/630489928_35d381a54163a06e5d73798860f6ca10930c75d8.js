
const url = 'https://brotherblazzard.github.io/canvas-content/fruit.json';

let myParent = document.getElementById("myParent");

const getFruits = async () => {
    const response = await fetch(url);
    result = await response.json();
   
let selectList = document.createElement("select");
let two = document.createElement("select");
let three = document.createElement("select");
selectList.id = "mySelect";
two.id = "two";
three.id= "three";
myParent.appendChild(selectList);
myParent.appendChild(two);
myParent.appendChild(three);

for (let i = 0; i < result.length; i++) {
    let option = document.createElement("option");
     let br = document.createElement('br');   
    let optionTwo = document.createElement("option");
    let optionThree = document.createElement("option");
    option.value = result[i].name;
    option.text = result[i].name;
    option.dataset.carbohydrates = result[i].nutritions.carbohydrates;
    option.dataset.protein = result[i].nutritions.protein;
    option.dataset.fat = result[i].nutritions.fat;
    option.dataset.sugar = result[i].nutritions.sugar;
    option.dataset.calories = result[i].nutritions.calories;

    optionTwo.value = result[i].name;
    optionTwo.text = result[i].name;
    optionTwo.dataset.carbohydrates = result[i].nutritions.carbohydrates;
    optionTwo.dataset.protein = result[i].nutritions.protein;
    optionTwo.dataset.fat = result[i].nutritions.fat;
    optionTwo.dataset.sugar = result[i].nutritions.sugar;
    optionTwo.dataset.calories = result[i].nutritions.calories;

    optionThree.value = result[i].name;
    optionThree.text = result[i].name;
    optionThree.dataset.carbohydrates = result[i].nutritions.carbohydrates;
    optionThree.dataset.protein = result[i].nutritions.protein;
    optionThree.dataset.fat = result[i].nutritions.fat;
    optionThree.dataset.sugar = result[i].nutritions.sugar;
    optionThree.dataset.calories = result[i].nutritions.calories;
    selectList.appendChild(option);
    two.appendChild(optionTwo);
    three.appendChild(optionThree);
}

};

getFruits();

let save = ()=>{
    let firstname = document.getElementById("fname").value;
    let email = document.getElementById("email").value;
    let phone = document.getElementById("phone").value;
    let fruitOne = document.getElementById("mySelect").value;
    let fruitTwo = document.getElementById("two").value;
    let fruitThree = document.getElementById("three").value;
    let instruction = document.getElementById("desc").value;
    let date = new Date().toLocaleDateString();
   
    const info = {
        firstname,
        email,
        fruitOne,
        fruitTwo,
        fruitThree,
        phone,
        instruction,
        date
    }
   
    const cart = window.localStorage.getItem('cart');

    if(cart === null) {
        window.localStorage.setItem('cart', JSON.stringify([info]));
    } else {
        const getCurrentCart = window.localStorage.getItem('cart');
        const currentCart = JSON.parse(getCurrentCart);
        currentCart.push(info);
        window.localStorage.setItem('cart', JSON.stringify(currentCart));
    }
    const resultArr = JSON.parse(window.localStorage.getItem("cart"));
    let lastData = resultArr[resultArr.length-1];
    console.log(lastData);
    document.getElementById("f_name").value = lastData.firstname;
    document.getElementById("e_mail").value = lastData.email;
    document.getElementById("p_hone").value = lastData.phone;
    document.getElementById("f_ruit").value = lastData.fruitOne;
    document.getElementById("f_ruit2").value = lastData.fruitTwo;
    document.getElementById("f_ruit3").value = lastData.fruitThree;
    document.getElementById("instruction").value = lastData.instruction;
    document.getElementById("order_date").value = lastData.date;
   document.querySelector(".form").style.display="none";
   document.querySelector(".checkoutData").style.display="block";

let carbhydrate = document.getElementById("mySelect")
let protein = document.getElementById("mySelect")
let fat = document.getElementById("mySelect")
let sugar = document.getElementById("mySelect")
let calories = document.getElementById("mySelect")

let carbo_value = carbhydrate.options[carbhydrate.selectedIndex].getAttribute('data-carbohydrates');
let protein_value = protein.options[protein.selectedIndex].getAttribute('data-protein');
let fat_value = fat.options[fat.selectedIndex].getAttribute('data-fat');
let sugar_value = sugar.options[sugar.selectedIndex].getAttribute('data-sugar');
let calories_value = calories.options[calories.selectedIndex].getAttribute('data-calories');


let carbhydrateTwo = document.getElementById("two")
let proteinTwo = document.getElementById("two")
let fatTwo = document.getElementById("two")
let sugarTwo = document.getElementById("two")
let caloriesTwo = document.getElementById("two")

let carbo_valueTwo = carbhydrateTwo.options[carbhydrateTwo.selectedIndex].getAttribute('data-carbohydrates');
let protein_valueTwo = proteinTwo.options[proteinTwo.selectedIndex].getAttribute('data-protein');
let fat_valueTwo = fatTwo.options[fatTwo.selectedIndex].getAttribute('data-fat');
let sugar_valueTwo = sugarTwo.options[sugarTwo.selectedIndex].getAttribute('data-sugar');
let calories_valueTwo = caloriesTwo.options[caloriesTwo.selectedIndex].getAttribute('data-calories');


let carbhydrateThree = document.getElementById("three")
let proteinThree = document.getElementById("three")
let fatThree = document.getElementById("three")
let sugarThree = document.getElementById("three")
let caloriesThree = document.getElementById("three")

let carbo_valueThree = carbhydrateThree.options[carbhydrateThree.selectedIndex].getAttribute('data-carbohydrates');
let protein_valueThree = proteinThree.options[proteinTwo.selectedIndex].getAttribute('data-protein');
let fat_valueThree = fatThree.options[fatThree.selectedIndex].getAttribute('data-fat');
let sugar_valueThree = sugarThree.options[sugarThree.selectedIndex].getAttribute('data-sugar');
let calories_valueThree = caloriesThree.options[caloriesThree.selectedIndex].getAttribute('data-calories');

const totalCarbohydrates = carbo_value +  carbo_valueTwo + carbo_valueThree;
const totalProtein = protein_value + protein_valueTwo + protein_valueThree;
const totalFat = fat_value + fat_valueTwo + fat_valueThree;
const totalSugar = sugar_value + sugar_valueTwo + sugar_valueThree;
const totalCalories = calories_value + calories_valueTwo + calories_valueThree;
document.getElementById("carb").innerHTML = totalCarbohydrates;
document.getElementById("prot").innerHTML = totalProtein;
document.getElementById("fat").innerHTML = totalFat;
document.getElementById("sug").innerHTML = totalSugar;
document.getElementById("cal").innerHTML = totalCalories;
}

let createNum =()=>{
    let numItems = document.getElementById("num_items");
    let number = JSON.parse(window.localStorage.getItem("cart")).length;
   
     numItems.innerHTML = `Number of order : ${number}`;
     return numItems;
}

createNum();



 
