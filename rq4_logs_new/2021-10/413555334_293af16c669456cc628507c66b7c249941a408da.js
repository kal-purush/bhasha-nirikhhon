"use strict";

let allLetters = [];
const textString = /* document.querySelector("#coolText").textContent */ "This text should fade in from the bottom";

window.addEventListener("DOMContentLoaded", start);

function start() {
  getLetters();
}

function getLetters() {
  allLetters = textString.split("");
  //console.log(allLetters);

  createSpan();
}

function createSpan() {
  allLetters.forEach((letter) => {
    //console.log(letter);
    let span = document.createElement("span");
    span.innerHTML = letter;
    document.querySelector("section").appendChild(span);
  });

  //makeSpanSpaces();
  animateLetters();
}

/* function makeSpanSpaces() {
  document.querySelectorAll("span").forEach((spanLetter) => {
    if (spanLetter.textContent == "") {
      spanLetter.style.marginRight = "5px";
      //animateLetters();
    } else {
      spanLetter.style.marginRight = "0px";
      //animateLetters();
    }
  });
} */

function animateLetters() {
  document.querySelectorAll("span").forEach((spanLetter, i) => {
    spanLetter.classList.add("moveLetters");
    spanLetter.style.animationDelay = `${i / 10}s`;
  });
}