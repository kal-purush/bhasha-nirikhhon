
function setId() {
    var hiddenField = document.getElementById('id')
    hiddenField.setAttribute("type", "hidden");
    hiddenField.setAttribute("name", "id");
    hiddenField.setAttribute("value", id);
}

setTimeout(function() {setId()}, 100);