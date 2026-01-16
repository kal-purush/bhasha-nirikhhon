const input = document.querySelector("input");
const button = document.querySelector("button");
const description = document.querySelector(".description");

button.addEventListener("click", () => {
  if (!input.value) {
    description.innerHTML = "Please enter a vowels";
  } else {
    vowelsString();
  }
});

const vowelsString = () => {
  const vowels = ["a", "e", "ı", "i", "o", "ö", "u", "ü"];
  const result = input.value
    .toLowerCase()
    .split("")
    .filter((letter) => vowels.includes(letter)).length;
    description.innerHTML = `There are ${result} vowels in <span style="color: red">${input.value.toLowerCase()}`;
    input.value="";
};