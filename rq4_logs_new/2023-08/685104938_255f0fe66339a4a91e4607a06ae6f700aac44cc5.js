// Adding event listener
// One way

// document.querySelector(".w").addEventListener("click",handleEvent);
// function handleEvent(){
//   alert("I got clicked!!")
// }

// Second and important way

// document.querySelector("button").addEventListener("click",function (){
//   alert("I got Clicked!!");
// });

// ------------------ //

// Adding loop.. to get addEventListener for all buttons..
var btnLen=document.querySelectorAll("button").length;
//alert(btnLen);
for(var i=0;i<btnLen;i++){
  document.querySelectorAll("button")[i].addEventListener("click",function(){
   console.log(this);
  // this.style.color="white";
  var buttonInnerHtml = this.innerHTML;
  //alert(document.querySelectorAll("button")[i]);
  console.log(buttonInnerHtml);
makeSounds(buttonInnerHtml);
buttonAnimation(buttonInnerHtml);
  });
}
document.addEventListener("keypress",function(event){
  makeSounds(event.key);
  buttonAnimation(event.key)
});
// for audio
// var aud= new Audio("sounds/tom-1.mp3");
// aud.play();

// Making sounds

function makeSounds(key){
  switch(key){
  case 'w':
  var crash= new Audio("sounds/crash.mp3");
  crash.play();
  break;
  case 'a':
  var kickbass= new Audio("sounds/kick-bass.mp3");
  kickbass.play();
  break;
  case 's':
  var snare= new Audio("sounds/snare.mp3");
  snare.play();
  break;
  case 'd':
  var tom1= new Audio("sounds/tom-1.mp3");
  tom1.play();
  break;
  case 'j':
  var tom2= new Audio("sounds/tom-2.mp3");
  tom2.play();
  break;
  case "k":
  var tom3= new Audio("sounds/tom-3.mp3");
  tom3.play();
  break;
  case 'l':
  var tom4= new Audio("sounds/tom-4.mp3");
  tom4.play();
  break;
  default:
  console.log(buttonInnerHtml);
}
}

// button animations

function buttonAnimation(pressedkey){
  var activeButton= document.querySelector("."+pressedkey);
  activeButton.classList.add("pressed");
  setTimeout(function(){
    activeButton.classList.remove("pressed");
  },100);
}


//// // Dr's code....
// var numberOfDrumButtons = document.querySelectorAll(".drum").length;
//
// for (var i = 0; i < numberOfDrumButtons; i++) {
//
//   document.querySelectorAll(".drum")[i].addEventListener("click", function() {
//
//     var buttonInnerHTML = this.innerHTML;
//
//     makeSound(buttonInnerHTML);
//
//     buttonAnimation(buttonInnerHTML);
//
//   });
//
// }
//
// document.addEventListener("keypress", function(event) {
//
//   makeSound(event.key);
//
//   buttonAnimation(event.key);
//
// });
//
//
// function makeSound(key) {
//
//   switch (key) {
//     case "w":
//       var tom1 = new Audio("sounds/tom-1.mp3");
//       tom1.play();
//       break;
//
//     case "a":
//       var tom2 = new Audio("sounds/tom-2.mp3");
//       tom2.play();
//       break;
//
//     case "s":
//       var tom3 = new Audio('sounds/tom-3.mp3');
//       tom3.play();
//       break;
//
//     case "d":
//       var tom4 = new Audio('sounds/tom-4.mp3');
//       tom4.play();
//       break;
//
//     case "j":
//       var snare = new Audio('sounds/snare.mp3');
//       snare.play();
//       break;
//
//     case "k":
//       var crash = new Audio('sounds/crash.mp3');
//       crash.play();
//       break;
//
//     case "l":
//       var kick = new Audio('sounds/kick-bass.mp3');
//       kick.play();
//       break;
//
//
//     default: console.log(key);
//
//   }
// }
//
//

//
// function buttonAnimation(currentKey) {
//
//   var activeButton = document.querySelector("." + currentKey);
//
//   activeButton.classList.add("pressed");
//
//   setTimeout(function() {
//     activeButton.classList.remove("pressed");
//   }, 100);
//
// }