//constants
const buttons = document.querySelectorAll('.btn')
let u_p = 0;
let c_p = 0;


//function

function playRound(e){
    const move = e.target;
    const types = move.dataset.typing;
    console.log(types);

    battleRound(types,);


    // grass_ele
    // fire_ele
    // water_ele
}


function battleRound(playerSelection, computerSelection){
    computerSelection = Math.floor(Math.random()*3);
    let playerSelection_hand = playerSelection.toLowerCase();
    let computerSelection_hand = "";
    
    
    switch (computerSelection){
        case 0:
            computerSelection_hand = "fire_ele";
            break;
        case 1:
            computerSelection_hand = "water_ele";
            break;
        case 2:
            computerSelection_hand = "grass_ele";
            break;
    } 
    if (playerSelection_hand == "fire_ele"||playerSelection_hand == "water_ele" || playerSelection_hand == "grass_ele"){
        if (computerSelection_hand == "water_ele" && playerSelection_hand == "fire_ele"){
            c_p += 1;
            console.log(`You lost!`);
            return c_p;
        } else if (computerSelection_hand == "fire_ele" && playerSelection_hand == "grass_ele"){
            c_p += 1;
            console.log(`You lost!`);
            return c_p;
        } else if (computerSelection_hand == "grass_ele" && playerSelection_hand == "water_ele"){
            c_p += 1;
            console.log(`You lost!`);
            return c_p;
        } else if (computerSelection_hand == playerSelection_hand){
            console.log(`It's a tie!`);
            u_p += 1;
            console.log(`You won!`);
            return u_p;
        }
    } else{
        return "Invalid input. Please type in Rock, Paper, or Scissor. Thank you!";
    }
      







// // event listeners

buttons.forEach(button => button.addEventListener('click', playRound));


// // let u_p = 0;
// // let c_p = 0;




// // function playRound(playerSelection, computerSelection){
// //     computerSelection = Math.floor(Math.random()*3);
// //     let playerSelection_hand = playerSelection.toLowerCase();




// // let u_p = 0;
// // let c_p = 0;




  
// }

// function game(){
//     for (let i = 1; i < 6; i++){
//         let input = prompt("What hand are you throwing? Paper, Rock or Scissor? ")
//         input = input.toString();

//         playRound(input);
//     }
// }

// // start //

// game();

// let score = `Your score is ${u_p} and the computer is ${c_p}.`;

// if (u_p > c_p){
//     console.log ("You win the game! "+ score);
// } else if(u_p < c_p){
//     console.log ("You lost the game! "+ score);
// } else {
//     console.log ("You draw the game! "+ score);
// }or? ")
//         input = input.toString();

//         playRound(input);
//     }
// }

// // start //

// game();

// let score = `Your score is ${u_p} and the computer is ${c_p}.`;

// if (u_p > c_p){
//     console.log ("You win the game! "+ score);
// } else if(u_p < c_p){
//     console.log ("You lost the game! "+ score);
// } else {
//     console.log ("You draw the game! "+ score);
// }