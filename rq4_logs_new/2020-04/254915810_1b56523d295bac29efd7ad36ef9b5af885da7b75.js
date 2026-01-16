var shadowElement = document.createElement("div");
shadowElement.className = "shadow";
shadowElement.innerText = "Here...";

function addColumn() {
  let output = ` <li class="column"><h3 contenteditable="true"class="column__heading">Things To Do</h3></li>`;
  document.getElementById("column-list").innerHTML += output;
}

const items = document.querySelectorAll(".item");
const emptyItem = document.querySelector(".empty-item");

items.forEach((item) => {
  item.addEventListener("dragstart", dragStart);
  item.addEventListener("dragend", dragEnd);
  item.addEventListener("dragover", dragOver);
  item.addEventListener("dragenter", dragEnter);
  item.addEventListener("dragleave", dragLeave);
  item.addEventListener("drop", dragDrop);
});

function dragStart(e) {
  this.className += " dragging";
  console.log("start");
  setTimeout(() => {
    e.target.remove();
  }, 0);
}

function dragEnd() {
  console.log("end");

  setTimeout(() => {
    document.querySelector(".dragging").classList.remove("dragging");
  }, 0);
}

function dragOver(e) {
  e.preventDefault();
  e.target.classList.add("check");
  console.log("over");
  // document.querySelector(".test").style.display = "none";
  const parentElement = document.querySelector(".list");
  // const pickedElement = document.querySelector(".dragging");
  const overElement = document.querySelector(".check");
  document.querySelector(".check").style.marginBottom = "60px";
  document.querySelector(".check").style.transition = "2s";
  // const measurement = overElement.getBoundingClientRect();

  // if (measurement.top + measurement.height / 2 > e.clientY) {
  //   parentElement.insertBefore(shadowElement, overElement);
  // } else {
  //   console.log("after");
  //   overElement.insertAdjacentElement("afterend", shadowElement);
  // }
  const measurement = e.target.getBoundingClientRect();

  // if (measurement.top + measurement.height / 2 > e.clientY) {
  //   parentElement.insertBefore(shadowElement, overElement);
  // } else {
  //   overElement.insertAdjacentElement("afterend", shadowElement);
  // }
}

function dragEnter(e) {
  console.log("enter");
  // const parentElement = document.querySelector(".list");
  // const enteredElement = document.querySelector(".entered");
  // parentElement.insertBefore(shadowElement, enteredElement.nextSibling);
}

function dragLeave(e) {
  console.log("leave");
  document.querySelector(".check").style.marginBottom = "20px";
  e.target.classList.remove("check");
}

function dragDrop(e) {
  e.target.classList.remove("check");

  const pickedElement = document.querySelector(".dragging");
  const parentElement = document.querySelector(".list");
  const measurement = e.target.getBoundingClientRect();

  if (measurement.top + measurement.height / 2 > e.clientY) {
    parentElement.insertBefore(pickedElement, e.target);
  } else {
    e.target.insertAdjacentElement("afterend", pickedElement);
  }

  e.target.classList.remove("dragging");
}