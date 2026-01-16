const tab1 = document.querySelector(".title-one");
const tab2 = document.querySelector(".title-two");
const tab3 = document.querySelector(".title-three");

const text1 = document.querySelector(".text-one");
const text2 = document.querySelector(".text-two");
const text3 = document.querySelector(".text-three");

tab1.addEventListener("click", () => {
  changeDisplayText(text2, text3, text1);
  changeClass(tab2, tab3, tab1);
});

tab2.addEventListener("click", () => {
  changeDisplayText(text1, text3, text2);
  changeClass(tab1, tab3, tab2);
});

tab3.addEventListener("click", () => {
  changeDisplayText(text1, text2, text3);
  changeClass(tab1, tab2, tab3);
});

function changeClass(one, two, three) {
  one.classList.remove("active");
  two.classList.remove("active");
  three.classList.add("active");
}

function changeDisplayText(one, two, three) {
  one.style.display = "none";
  two.style.display = "none";
  three.style.display = "block";
}