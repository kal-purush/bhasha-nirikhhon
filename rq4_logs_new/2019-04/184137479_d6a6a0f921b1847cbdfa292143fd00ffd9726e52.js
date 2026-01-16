console.log("hello world. this is a test");

const idInput = document.getElementById("idInput");
const colorInput = document.getElementById("colorInput");

function setCard() {
  const card = document.getElementById(idInput.value);
  card.style.color = colorInput.value;
}

function reset() {
  var rocks = document.getElementById("diamonds");
  rocks.style.color = "grey";
  var puppyFeet = document.getElementById("clubs");
  puppyFeet.style.color = "grey";
  var shovels = document.getElementById("spades");
  shovels.style.color = "grey";
  var blood = document.getElementById("hearts");
  blood.style.color = "grey";
}