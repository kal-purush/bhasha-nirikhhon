console.log("howdy doodle!");

const btn = document.querySelector("a-button");

const colorBtn = document.getElementById("a-button");

function changeColor() {
  if (document.body.style.background == "darkseagreen") {
    document.body.style.background = "orange";
  } else {
    document.body.style.background = "darkseagreen";
  }
}

colorBtn.addEventListener("click", changeColor);

// const btn = document.querySelector("button");

// function random(number) {
//   return Math.floor(Math.random() * (number + 1));
// }

// btn.addEventListener("click", () => {
//   const rndCol = `rgb(${random(255)}, ${random(255)}, ${random(255)})`;
//   document.body.style.backgroundColor = rndCol;
// });