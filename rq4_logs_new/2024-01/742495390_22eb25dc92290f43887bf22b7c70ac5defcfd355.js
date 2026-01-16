// Faire disparaître la calculatrice
const appearButton = document.querySelector(".appear-link");
const disappearButton = document.querySelector(".disappear-link");
const calculatrice = document.querySelector("form");

appearButton.addEventListener("click", (e) => {
  e.preventDefault();
  calculatrice.style.visibility = "visible";
  appearButton.classList.add("hover");
});

disappearButton.addEventListener("click", (e) => {
  e.preventDefault();
  calculatrice.style.visibility = "hidden";
});

// Hover
const inputsHover = document.querySelectorAll("form > input");
const operator = document.querySelector("select");
const toAppear = document.querySelector(".react-hover");

inputsHover.forEach((element) => {
  element.addEventListener("mouseenter", () => {
    toAppear.classList.add("react-hover-active");
    toAppear.textContent = "Saisir un chiffre";
  });
  element.addEventListener("mouseleave", () => {
    toAppear.classList.remove("react-hover-active");
  });
});

operator.addEventListener("mouseenter", () => {
  toAppear.classList.add("react-hover-active");
  toAppear.textContent = "Selectionner un operateur";
});

operator.addEventListener("mouseleave", () => {
  toAppear.classList.remove("react-hover-active");
});

//Logique de calcul
function calcul(numberA, numberB, operator) {
  if (operator === "addition") {
    const result = +numberA + +numberB;
    return `${numberA} +  ${numberB} = ${result}`;
  }
  if (operator === "soustraction") {
    const result = +numberA - +numberB;
    return `${numberA} -  ${numberB} = ${result}`;
  }
  if (operator === "multiplication") {
    const result = +numberA * +numberB;
    return `${numberA} *  ${numberB} = ${result}`;
  }
  if (operator === "division") {
    const result = +numberA / +numberB;
    return `${numberA} /  ${numberB} = ${result}`;
  }
}

function resultToAppear(numberA, numberB, operator) {
  const resultAppear = document.querySelector(".answer-container");
  const textResult = document.querySelector(".answer-container");

  resultAppear.classList.add("answer-container-active");
  resultAppear.textContent = calcul(numberA, numberB, operator);
  textResult.classList.add("answer-container-active");
}

calculatrice.addEventListener("submit", (e) => {
  e.preventDefault();
  const numberA = calculatrice.elements["first-input"].value;
  const numberB = calculatrice.elements["second-input"].value;
  resultToAppear(numberA, numberB, operator.value);
});

const reset = document.querySelector(".reset");
reset.addEventListener("click", () => {
  const resultAppear = document.querySelector(".answer-container");
  const textResult = document.querySelector(".answer-container");
  resultAppear.classList.remove("answer-container-active");
  textResult.classList.remove("answer-container-active");
});