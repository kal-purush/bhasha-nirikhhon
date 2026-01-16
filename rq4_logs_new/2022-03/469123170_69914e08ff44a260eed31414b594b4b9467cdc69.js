//enter press show result
const searchBtn = document.getElementById("button-search");
const searchField = document.getElementById("search-field");
searchField.addEventListener("keypress", function (event) {
    if (event.keyCode == 13) {
        searchBtn.click();
    }
});

const searchFood = () => {
    const searchField = document.getElementById("search-field");
    const searchText = searchField.value;
    if (searchText == "") {
        return alert("Write your text what you need");
    }

    console.log(searchText);
    //clear data
    searchField.value = "";
    //load data
    const url = `https://www.themealdb.com/api/json/v1/1/search.php?s=${searchText}`;
    fetch(url)
        .then(res => res.json())
        .then(data => displaySearchResult(data.meals))
}
const displaySearchResult = meals => {
    const searchResult = document.getElementById("search-result");
    searchResult.textContent = "";
    if (meals.length == 0) {
        return alert("It's Not available for now")
    }
    meals.forEach(meal => {
        console.log(meal);
        const div = document.createElement("div");
        div.classList.add("col");
        div.innerHTML = `
        <div onclick="loadMealDetil(${meal.idMeal})" class="card h-100">
                <img src="${meal.strMealThumb}" class="card-img-top" alt="...">
                <div class="card-body">
                <h5 class="card-title">${meal.strMeal}</h5>
                <p class="card-text">${meal.strInstructions.slice(0, 200)}</p>
            </div>
        </div>
        `;
        searchResult.appendChild(div);
    });
}
const loadMealDetil = mealId => {
    console.log(mealId);
    const url = `https://www.themealdb.com/api/json/v1/1/lookup.php?i=${mealId}`;
    fetch(url)
        .then(res => res.json())
        .then(data => displayMealDetail(data.meals[0]))
}
const displayMealDetail = meal => {
    // console.log(meal);
    const mealDetails = document.getElementById("meal-details");
    mealDetails.textContent = "";
    const div = document.createElement("div");
    div.classList.add("card");
    div.innerHTML = `
    <img src="${meal.strMealThumb}" class="card-img-top" alt="...">
            <div class="card-body">
            <h5 class="card-title">${meal.strMeal}</h5>
            <p class="card-text">${meal.strInstructions.slice(0, 150)}</p>
            <a href="${meal.strYoutube}" class="btn btn-primary">Go somewhere</a>
            </div>
    `;
    mealDetails.appendChild(div);
}   