const muz = document.querySelectorAll(".muz");
for (let i = 0; i < muz.length; i++) 
muz[i].addEventListener("click", () => {
let btnText = muz[i].textContent;
playMusic (btnText)
animation (btnText)
});
document.addEventListener('keypress',(e)=> {
let keytext = e.key
playMusic (keytext)
animation (keytext)
})
function playMusic(key) {
switch (key) {
case "w": 
let dunyo = new Audio("sounds/crash.mp3");
dunyo.play();
break;
case "a":
let kay = new Audio("sounds/kick-bass.mp3");
kay.play();
break;
case "s":
let sof = new Audio("sounds/snare.mp3");
sof.play();
break;
case "d":
let al = new Audio("sounds/tom-1.mp3");
al.play();
break;
case "j":
let kar = new Audio("sounds/tom-2.mp3");
kar.play();
break;
case "k":
let ran = new Audio("sounds/tom-3.mp3");
ran.play();
break;
case "l":
let vini = new Audio("sounds/tom-4.mp3");
vini.play();
break;
default:
console.log(btnText);
break;
}}


function animation(key) {
const pressed = document.querySelector(`.${key}`)
pressed.classList.add('pressed')
setTimeout(()=> {
pressed.classList.remove('pressed')
}, 100)
}