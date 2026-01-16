const player1=document.getElementById("player1")
const computer=document.getElementById("player2")
const paper=document.getElementById("paper")
const rock=document.getElementById("rock")
const scissors=document.getElementById("scissoers")
const ops=["","paper","rock","scissors"]
let points=0
let compoints=0
let Useracsion
paper.addEventListener("click",()=>{
    Useracsion="paper"
    main() 
})
rock.addEventListener("click",()=>{
    Useracsion="rock"
    // console.log("clcked")
    main() 
})
scissors.addEventListener("click",()=>{
    Useracsion="scissors"
    main() 
})
function main() {
    if (points==5 || compoints==5){
        document.getElementById("result").style.display="block"
        document.querySelector(".buttons").style.display="none"
        if (points>compoints){
            document.getElementById("player").innerHTML="You Won!!"
            console.log(document.getElementById("player"))
        }
        else{
            document.getElementById("player").innerHTML="computer Won!!"
            console.log(document.getElementById("player"))
        }
    }
    

else{
    let computerops=ops[Math.ceil(Math.random()*3)]
    player1.src=`./assets/${Useracsion}-hand.png`
    computer.src=`./assets/${computerops}-hand.png`
    if (Useracsion==computerops){
    }
    else  if(Useracsion=="paper" && computerops=="rock" || Useracsion=="scissors" && computerops=="rock" || Useracsion=="rock" && computerops=="paper"){
        compoints++
        document.getElementById("computer").innerHTML=compoints
    }
    else{
        points++
        document.getElementById("user").innerHTML=points
    }
}}
document.getElementById("button").addEventListener("click",()=>{location.reload("./index.html")})