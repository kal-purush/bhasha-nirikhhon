//This function is to get the users name
function getName() {

    let myName = prompt("What is your name?");
    alert("Thank you, " + myName + " . Welcome to the Leannatrix");
}

//This function asks the user if they would like to play a game.
function askAboutGame()

let gameResult = confirm(myName + "would you like to play a game?");
if (gameResult == true) {
    alert("wooo! Let's get started, " + myName + ". Make sure to answer the questions with a yes or a no.");
 

let year = prompt("Was Leanna born in 1995?");
   
    if (year == "yes" || "y") {
    alert("No, she wasn't! Try again");

    } else if (year == "no" || "n") {
        alert("Well done! Leanna was born in 1997, not 1995.");
    } else {
        alert("Please answer yes or no.");
    }

let placeOfBirth = prompt("Was Leanna born in Romford?");
   
    if (placeOfBirth == "yes" || "y") {
    alert("Well done, Leanna was unfortunately born in Romford.");

    } else if (placeOfBirth == "no" || "n") {
        alert("Leanna wishes, please try again.");
    } else {
        alert("Please answer yes or no.");
    }

let awesomeAnswer = prompt("Is Leanna the most awesome person in the world?");
   
    if ( awesomeAnswer == "yes" || "y") {
    alert("Hell yeaaaaaah, that is correct.");

    } else if (awesomeAnswer == "no" || "n") {
        alert("Get the hell off of my page...NOW!");
    } else {
        alert("Please answer yes or no.");
    }    

} else {
    alert("Wow, ok then...Bye " + myName);
} 

getName ();
askAboutGame ();
