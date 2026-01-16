function buildDropDowns(sizes) {
  var selections = document.getElementsByClassName("resourceSize");
  for (var i = 0; i < selections.length; i++) {
    //console.log(selection);
    for (var j = 0; j < sizes.length; j++) {
      selections[i].options.add(createOption(sizes[j], sizes[j]));
    }
  }
}

function createOption(option, label) {
  var option = document.createElement("option");
  option.setAttribute("value", option);
  option.innerHTML = label;

  return option;
}

var sizes = ["XXL", "XL", "L", "M", "S", "XS", "M", "hk"];

buildDropDowns(sizes);