var elShoppingForm = document.querySelector(".shopping-form");
var elShoppingInput = elShoppingForm.querySelector(".shopping-input");
var elShoppingButton = elShoppingForm.querySelector(".shopping-btn");
var elShoppingCheck = document.querySelector(".checkbox");
var elShoppingList = document.querySelector(".shopping-list");


var elShoppingFormCheck = document.querySelector(".shopping-form_check");
var elShoppingInputCheck = document.querySelector(".shopping-input_check");
var elShoppingButtonCheck = document.querySelector(".shopping-btn_check");
var elShoppingListCheck = document.querySelector(".shopping-list_check");

var shoppingList = [];

elShoppingForm.addEventListener("submit", function (e) {
    e.preventDefault();

    shoppingList.push(elShoppingInput.value.trim());
    elShoppingInput.focus();

    elShoppingList.innerHTML = "";
    for (goods of shoppingList) {
        var newAddedList = document.createElement("li");
        newAddedList.textContent = goods;
        elShoppingList.appendChild(newAddedList);

    }

    elShoppingInput.value = null;
})

elShoppingFormCheck.addEventListener("submit", function (evt) {
    evt.preventDefault();

    var checkInput = elShoppingInputCheck.value.trim();
        if (shoppingList.includes(checkInput)) {
            console.log("bor")
        } else {
            console.log("yo'q")
        }
})