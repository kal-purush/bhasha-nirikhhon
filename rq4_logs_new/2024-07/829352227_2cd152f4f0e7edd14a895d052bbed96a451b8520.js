

const url = "http://localhost:3000/meals";

document.addEventListener("DOMContentLoaded", function() {
    let allMeals = [];

    // Function to display the list of recipes
    function displayRecipes(meals) {
        const recipesContainer = document.getElementById("recipes");
        recipesContainer.innerHTML = ""; // Clear previous recipes
        
        // Loop through each meal and create HTML elements to display its details
        meals.forEach(meal => {
            const recipeDiv = document.createElement("div");
            recipeDiv.classList.add("recipe");

            const recipeImg = document.createElement("img");
            recipeImg.src = meal.food_img;
            recipeImg.alt = meal.foodname;
            recipeDiv.appendChild(recipeImg);

            const recipeTitle = document.createElement("h2");
            recipeTitle.textContent = meal.foodname;
            recipeDiv.appendChild(recipeTitle);

            const ingredientsTitle = document.createElement("h3");
            ingredientsTitle.textContent = "Ingredients:";
            recipeDiv.appendChild(ingredientsTitle);

            const ingredientsList = document.createElement("ul");
            meal.ingredients.forEach(ingredient => {
                const ingredientItem = document.createElement("li");
                ingredientItem.textContent = ingredient;
                ingredientsList.appendChild(ingredientItem);
            });
            recipeDiv.appendChild(ingredientsList);

            const directionsTitle = document.createElement("h3");
            directionsTitle.textContent = "Directions:";
            recipeDiv.appendChild(directionsTitle);

            const directions = document.createElement("p");
            directions.textContent = meal.Directions;
            recipeDiv.appendChild(directions);

            const videoLink = document.createElement("a");
            videoLink.href = meal["video-reference"];
            videoLink.target = "_blank";
            videoLink.textContent = "Watch Video";
            recipeDiv.appendChild(videoLink);

            recipesContainer.appendChild(recipeDiv);
        });
    }

    // Fetch the meal data from the server
    fetch(url)
        .then(response => response.json()) // Parse the JSON response
        .then(data => {
            allMeals = data; // Store the fetched data in allMeals
            displayRecipes(allMeals); // Display all recipes initially
        })
        .catch(error => {
            console.error("Error fetching data:", error); // Handle any errors during fetch
        });

    // Add event listener to the search button
    document.getElementById("searchButton").addEventListener("click", function() {
        const keyword = document.getElementById("keyword").value.toLowerCase(); // Get the search keyword
        // Filter meals based on the keyword
        const searchedMeals = allMeals.filter(meal =>
            meal.foodname.toLowerCase().includes(keyword) || // Check if foodname includes the keyword
            meal.ingredients.some(ingredient => ingredient.toLowerCase().includes(keyword)) || // Check if any ingredient includes the keyword
            meal.Directions.toLowerCase().includes(keyword) // Check if directions include the keyword
        );
        displayRecipes(searchedMeals); // Display the filtered recipes
    });
});