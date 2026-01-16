var list = document.getElementById('list');
var form = document.getElementById('form');
var input = document.getElementById('input');
document.addEventListener("submit", function (e) {
    e.preventDefault();
    if ((input === null || input === void 0 ? void 0 : input.value) == "" || (input === null || input === void 0 ? void 0 : input.value) == null) {
        console.log("no input");
        return;
    }
    console.log("input: " + input.value);
    addListItem(input.value);
});
function addListItem(location) {
    console.log("location" + location);
    var item = document.createElement("li");
    var label = document.createElement("label");
    var checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    label.append(checkbox, location);
    item.append(label);
    list === null || list === void 0 ? void 0 : list.append(item);
    console.log("list item appended");
}