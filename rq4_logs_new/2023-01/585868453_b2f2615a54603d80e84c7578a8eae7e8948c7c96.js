var ingredient_list = document.getElementById("ingredient-list");
var add_fields = document.getElementById("add-field");
var remove_fields = document.getElementById("remove-field");
console.log("test");

add_fields.onclick = function () {
  var newField = document.createElement("input");
  attrs = {
    type: "text",
    name: "ingredient[]",
    placeholder: "Add new field",
  };
  for (attr in attrs) {
    newField.setAttribute(attr, attrs[attr]);
  }
  ingredient_list.appendChild(newField);
};

remove_fields.onclick = function () {
  var input_fields = ingredient_list.getElementsByTagName("input");
  if (input_fields.length > 2) {
    ingredient_list.removeChild(input_fields[input_fields.length - 1]);
  }
};