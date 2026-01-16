console.log("yo, duh");

// "name": "Ravioli",
// "category": "pasta",
// "ethnicity": "italian",
// "id": 6,
// "restaurantId": 2

// fetch("http://localhost:8088/food")
// 	.then(foods => foods.json())
// 	.then(parsedFoods => {
// 		console.table(parsedFoods);
// 	});

function createFoodHTML(foodObj) {
	let foodHTML = `
    <section>
        <h3>Name: ${foodObj.name}</h3>
        <p>Category: ${foodObj.category}</p>
        <p>Ethnicity: ${foodObj.ethnicity}</p>
    </section>
    `;

	return foodHTML;
}

let foodList = document.querySelector(".foodList");

function addFoodtoDom(dogs) {
	foodList.innerHTML += dogs;
}

fetch("http://localhost:8088/food")
	.then(foods => foods.json())
	.then(parsedFoods => {
		parsedFoods.forEach(item => {
			const foodAsHTML = createFoodHTML(item);
			addFoodtoDom(foodAsHTML);
		});
	});