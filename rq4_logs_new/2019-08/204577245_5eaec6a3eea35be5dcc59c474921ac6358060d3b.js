let sonicPos = 1;

let knucklesPos= 1;

let eggmanPos=1;

const start =()=>{
   
let start=document.getElementById("start");

eggman=document.getElementById("eggman");

start.style.display= "none";

eggman.style.display= "block";

sonic.style.display= "block";

knuckles.style.display= 'block';



const addOne =()=>{

eggmanPos++;

eggman.style.marginleft= eggmanPos+'vw';

if(eggmanPos==100 && sonicPos <100 && knucklesPos <100){if (confirm('you both let eggman get away!!!)')){location.reload();}}

}

for(i=1;i<=100;i ++){

window.setInterval(addOne(), 500);

}

}



const sonic =()=>{

let sonic=document.getElementById("sonic");

SonicPos+=1;

sonic.style.marginLeft= sonicPos+'vw' ;

if (sonicPos==100){

if (confirm("Sonic has won the race!!")){location.reload();}

}

}



const knuckles =()=>{

let knuckles=document.getElementById("knuckles");

knucklesPos+=1;

knuckles.style.marginLeft= knucklesPos+'vw' ;

if (knucklesPos==100){

if (confirm("Knuckles has won the game!!")){location.reload();}

}

}
