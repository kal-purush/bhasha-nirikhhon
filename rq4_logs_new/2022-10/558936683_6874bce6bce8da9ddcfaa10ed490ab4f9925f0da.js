var deckid;
var PlayerPile;
var CpuPile;
var ThrowPile;
var temp;

const timer = ms => new Promise(res => setTimeout(res,ms))

window.onload = init;

function init(){
    GetDeck();
}
function GetDeck(){
fetch('https://www.deckofcardsapi.com/api/deck/new/shuffle/?deck_count=1')
  .then((response) => response.json())
  .then((data) => {
    deckid = data.deck_id;
    DivideCards();
    });
}

function DivideCards(){
    DrawCards();
}

function DrawCards(){
    fetch(`https://www.deckofcardsapi.com/api/deck/${deckid}/draw/?count=26`)
    .then((response) => response.json())
    .then((data) => {
        temp = data.cards;
        
        PlayerPile = data.cards;
        // for(let i =0;i<=9 ;i++){
        //     let cardImg = document.createElement("img");
        //     let card = PlayerPile[i];
        //     let imageUrl = card.images.png;
        //     // console.log(imageUrl);
        //     cardImg.src = imageUrl;
        //     document.getElementById("cpucards").append(cardImg);
        // }
        DrawRemaining();
    });
}

function DrawRemaining(){
    fetch(`https://www.deckofcardsapi.com/api/deck/${deckid}/draw/?count=26`)
    .then((response) => response.json())
    .then((data) => {
        
        CpuPile = data.cards;
        document.getElementById("cardp").disabled=false;
        document.getElementById("msg").innerHTML="Draw Your Card...";
    });
}
var count =0,pvalue,cvalue,result=0,cresult=0;
function DrawPlayerCard(){
    if(count<10){
    document.getElementById("cardp").disabled=true;
    let cardImg = document.createElement("img");
    let card = PlayerPile[count];
    let imageUrl = card.images.png;
    // console.log(imageUrl);
    cardImg.src = imageUrl;
    document.getElementById("playercards").append(cardImg);
    count++;
    pvalue = card.value;
    if(pvalue == "ACE"){
        pvalue = 1;
    }else if(pvalue == "JACK"){
        pvalue = 11
    }else if(pvalue == "QUEEN"){
        pvalue = 12;
    }else if(pvalue == "KING"){
        pvalue = 13;
    }else if(pvalue == 0){
        pvalue = 10;
    }
    
    DrawCpuCard();
    document.getElementById("msg").innerHTML="Player 2 is Drawing Card...";
    }
}

async function DrawCpuCard(){
    await timer(1000);
    let cardImg = document.createElement("img");
    let card = CpuPile[count];
    let imageUrl = card.images.png;
    // console.log(imageUrl);
    cardImg.src = imageUrl;
    document.getElementById("cpucards").append(cardImg);
    document.getElementById("cardp").disabled=false;

    cvalue = card.value;
    if(cvalue == "ACE"){
        cvalue = 1;
    }else if(cvalue == "JACK"){
        cvalue = 11
    }else if(cvalue == "QUEEN"){
        cvalue = 12;
    }else if(cvalue == "KING"){
        cvalue = 13;
    }else if(cvalue == 0){
        cvalue = 10;
    }

    if(pvalue > cvalue){
        result++;
        document.getElementById("ppoints").innerHTML=`Points: ${result}`;
    }else if(pvalue < cvalue){
        cresult++;
        document.getElementById("cpoints").innerHTML=`Points: ${cresult}`;
    }
    
    if(count == 10){
        FindResult();
    }else{
        document.getElementById("msg").innerHTML="Draw Your Card...";
    }
}

function FindResult(){
    if(result == 5){
        console.log("Draw");
        document.getElementById("msg").innerHTML="It was a Draw !";
    }else if(result>5){
        console.log("You Win");
        document.getElementById("msg").innerHTML="You Win !!";
    }else{
        console.log("You Lose");
        document.getElementById("msg").innerHTML="You Lose, Better Luck next Draw xD";
    }
}