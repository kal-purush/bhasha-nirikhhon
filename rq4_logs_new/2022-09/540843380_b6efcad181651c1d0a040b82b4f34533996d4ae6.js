// document.querySelector("button").addEventListener("click",handClicked);

// function handClicked(){
//     alert("Clicked !");
// } 

// document.querySelectorAll("button")[0].addEventListener("click",function(){
//     alert("I got clicked !");
// })
// document.querySelectorAll("button")[1].addEventListener("click",function(){
//     alert("I got clicked !");
// })
// document.querySelectorAll("button")[2].addEventListener("click",function(){
//     alert("I got clicked !");
// })
// document.querySelectorAll("button")[3].addEventListener("click",function(){
//     alert("I got clicked !");
// })
// document.querySelectorAll("button")[4].addEventListener("click",function(){
//     alert("I got clicked !");
// })
// document.querySelectorAll("button")[5].addEventListener("click",function(){
//     alert("I got clicked !");
// })
// document.querySelectorAll("button")[6].addEventListener("click",function(){
//     alert("I got clicked !");
// })


var numberOfDrums = document.querySelectorAll(".drum").length;
for(let i=0;i<numberOfDrums;i++){
    document.querySelectorAll("button")[i].addEventListener("click",function(){
        var ch = this.innerHTML;

        switch (ch) {
            case 'w':var audio = new Audio('sounds/crash.mp3');
            audio.play();
            break;
            case 'a':var audio = new Audio('sounds/kick-bass.mp3');
            audio.play();
                break;
            case 's':var audio = new Audio('sounds/snare.mp3');
            audio.play();
                break;
                case 'd':var audio = new Audio('sounds/tom-1.mp3');
            audio.play();
                break;
                case 'j':var audio = new Audio('sounds/tom-2.mp3');
            audio.play();
                break;
                case 'k':var audio = new Audio('sounds/tom-3.mp3');
            audio.play();
                break;
                case 'l':var audio = new Audio('sounds/tom-4.mp3');
            audio.play();
                break;
            default:
                break;
        }
    })
}
var audio = new Audio('sounds/tom-1.mp3');
        audio.play();