// Game States
const states = {
    Game: 0,
    Winner: 1,
    Loser: 2
}
let state = states.Game;

// Preload image files
let bg;
let title_1;
let title_2;
let poke_fire;
let poke_grass;
let poke_water;
function preload(){
    bg  = loadImage("images/bg.jpg")
    title_1 = loadImage("images/T2.png")
    title_2 = loadImage("images/T1.png")
    poke_fire = loadImage("images/Charzard.png")
    poke_grass = loadImage("images/vensaur.png")
    poke_water = loadImage("images/blastoise.png")

}

// Selection
const selection = [

    {
        name: "Charzard",
        image: poke_fire,
        beats: "Vensaur"
    },

    {
        name: "Blastoise",
        image: poke_water,
        beats:"Charzard"
    },

    {
        name: "Vensaur",
        image:  poke_grass,
        beats:  "Blastoise"
    } 
    
]



let player_selec;
let computer_selec;
let player_win_count = 0;
let comp_win_count = 0;

function randomSelec(){
    const Random = Math.floor(Math.random() * selection.length)
    return selection[Random];
}

function isWinner(){

}
function setup() {
    createCanvas(700,700);
    background(bg);
    rectMode(CENTER);

}

function draw() {
    computer_selec;

    image(title_1,150,60,400,75);
    image(title_2,160,130,400,100);

    // player deck
    fill(100,0,200,.75)
    rect(350,600,400,100,20)

    image(poke_fire,190,560,80,80);
    image(poke_water,305,560,80,80);
    image(poke_grass,420,560,80,80);

    fill("black")
    textSize(40);
    text(`Computer: ${comp_win_count}`,40,250)
    text(`Player: ${player_win_count}`,480,250)
    
}

function mouseClicked(){
    background(bg);
    if(mouseX > 190 && mouseX < 270 && mouseY < 680 && mouseY > 520){
        player_selec = selection[0];
        image(poke_fire,305,460,80,80);
        computer_selec = randomSelec(); 
        console.log(computer_selec.name);
       
    }
    else if(mouseX > 305 &&mouseX < 385 && mouseY < 680 && mouseY > 520){
        player_selec = selection[1],
        image(poke_water,305,460,80,80);
        computer_selec = randomSelec(); 
        console.log(computer_selec.name);
    }

    else if(mouseX > 420 && mouseX < 500 && mouseY < 680 && mouseY > 520){
        player_selec = selection[2],
        image(poke_grass,305,460,80,80);
        computer_selec = randomSelec(); 
        console.log(computer_selec.name);
    }

    if (player_selec.beats == computer_selec.name){
        player_win_count += 1;
    }

    else if(computer_selec.beats == player_selec.name){
        comp_win_count += 1;
    }

    if(computer_selec.name == "Blastoise"){
        image(poke_water,305,310,80,80);

    }
    else if(computer_selec.name == "Vensaur"){
        image(poke_grass,305,310,80,80);
    }
    else if(computer_selec.name == "Charzard"){
        image(poke_fire,305,310,80,80)
    }



}