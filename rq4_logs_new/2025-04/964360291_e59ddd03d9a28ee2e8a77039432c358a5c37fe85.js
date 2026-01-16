const text = "Hello, I'm learning web development!";
const typingElement = document.getElementById("typing");
let index = 0;

function typeText() {
  if (index < text.length) {
    typingElement.textContent += text.charAt(index);
    index++;
    setTimeout(typeText, 100); // Adjust typing speed here
  }
}

// typeText();

function toggleMenu() {
    const nav = document.querySelector('.navigation');
    nav.classList.toggle('active');
}

var loader = document.querySelector("#loader")
setTimeout(function(){
    loader.style.top = "-100%"
},8800)