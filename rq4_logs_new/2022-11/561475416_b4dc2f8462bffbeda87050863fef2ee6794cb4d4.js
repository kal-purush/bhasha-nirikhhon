function allowDrop(ev) {
    ev.preventDefault();
}
  
function drag(ev) {
    ev.dataTransfer.setData("text", ev.target.id);
}
  
function drop(ev) {
    ev.preventDefault();
    var data = ev.dataTransfer.getData("text");
    ev.target.appendChild(document.getElementById(data));
}

function empty() {
  const number = document.getElementById("number");

if (number.value != 828) {
    document.getElementById("errorNumber").innerHTML = " Feil input";
    return false;
  }
else {
    document.getElementById("errorNumber").innerHTML = "";
}
}

function emptyText() {
  const paste = document.getElementById("paste");
  const copy = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris vel orci tortor. Proin vel sollicitudin dui. Quisque et lorem at libero sagittis tincidunt vel vitae nisl. Etiam a odio mauris. Vestibulum a hendrerit ligula, sit amet tincidunt quam. Etiam ipsum nulla, ultricies non eros at, mollis gravida ipsum. Cras volutpat posuere nulla ac vulputate. Morbi sodales velit non sagittis bibendum. Aenean facilisis pharetra lacus.";

if (paste.value != copy) {
    document.getElementById("errorText").innerHTML = " * Feil input";
    return false;
  } else {
    document.getElementById("errorText").innerHTML = "";
  }
}

