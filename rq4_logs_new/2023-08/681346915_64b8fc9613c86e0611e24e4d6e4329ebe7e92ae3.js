var randomNumber1 = (Math.floor(Math.random()*6))+1;
var randomNumber2 = (Math.floor(Math.random()*6))+1;
document.querySelector(".imgLeft").setAttribute("src", "./images/dice"+randomNumber1+".png");
document.querySelector(".imgRight").setAttribute("src", "./images/dice"+randomNumber2+".png");

if (randomNumber1>randomNumber2){
    document.querySelector(".statement").textContent = "🏆 Player 1 won!"
}

else if (randomNumber2>randomNumber1){
    document.querySelector(".statement").textContent = "🏆 Player 2 won!";
}

else{
    document.querySelector(".statement").textContent = "🟰 It's a tie!"
}