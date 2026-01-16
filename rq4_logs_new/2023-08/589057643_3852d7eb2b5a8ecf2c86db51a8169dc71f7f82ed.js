let addIngredientsBtn = document.getElementById('addIngredients');
let ingredientsList = document.querySelector('.ingredientsList');
let ingredientDiv = document.querySelectorAll('.ingredientDiv')[0];

addIngredientsBtn.addEventListener('click', function() {
    let newIngredient = ingredientDiv.cloneNode(true);
    let input = newIngredient.getElementsByTagName('input')[0];
    input.value = '';
    ingredientsList.appendChild(newIngredient);
});