
alert("Welcome to Dice Game");

var randomNumber1 = Math.floor(Math.random()*6)+1;
// var array = ["images/dice1.png", "images/dice2.png", "images/dice3.png", "images/dice4.png", "images/dice5.png", "images/dice6.png"];
var sour = "images/dice"+randomNumber1+".png";
// document.getElementsByClassName("img1").src=array[randomNumber1];
document.querySelectorAll("img")[0].setAttribute("src", sour);

var randomNumber2 = Math.floor(Math.random()*6)+1;

var sour1 = "images/dice"+ randomNumber2 + ".png";
document.querySelectorAll("img")[1].setAttribute("src", sour1);

function output(){

if(randomNumber2>randomNumber1){
    return "player 2" ;
}
else if(randomNumber1>randomNumber2){
    return "player 1";
}
else{
    return " no one it's a tie";
}

}
document.querySelector("h2").innerHTML = "The winner is "+output() + ".";