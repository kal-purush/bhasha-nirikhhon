const express = require("express");
const router = express.Router();
const fs = require("fs");
const uniqid = require("uniqid");

function orderFoodDrinks() {
    const foodDrinksData = fs.readFileSync("./data/food-drinks-package.json");
    // console.log(foodData)
    const parsedData = JSON.parse(foodDrinksData);
    return parsedData;
}

router.use((_req, res, next) => {
    console.log("Middleware from the food router");
    next();
});

router.get("/", (_req, res) => {
    const foodDrinksData = fs.readFileSync("./data/food-drinks-package.json");
    // console.log(drinksData)
    const parsedData = JSON.parse(foodDrinksData);
    res.json(parsedData);
});



module.exports = router;