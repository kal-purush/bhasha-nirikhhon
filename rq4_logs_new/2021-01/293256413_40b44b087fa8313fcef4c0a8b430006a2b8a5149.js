var buttonColours = ["red","blue","green","yellow"];
var gamePattern = [];
var userClickedPattern = [];
var level1 = 0;
var started = true;

// Use jQuery to detect when any of the buttons are clicked and trigger a handler function.

$(".btn").click(function(){
    // Inside the handler, create a new variable called userChosenColour to store the id of the button that got clicked.
    var userChoosenColour = $(this).attr("id");

    // Add the contents of the variable userChosenColour created in step 2 to the end of this new userClickedPattern
    userClickedPattern.push(userChoosenColour);

    playsound(userChoosenColour);
    animatePress(userChoosenColour);                                            
    
    // Call checkAnswer() after a user has clicked and chosen their answer, passing in the index of the last answer in the user's sequence.
    checkAnswer(userClickedPattern.length-1);

    // console.log(userClickedPattern);
});

$("h1").click(function(event){
    if(started){
        $("h1").text("Level "+level1);
        nextSequence();
        started = false;
    }
});

function nextSequence(){
    // Once nextSequence() is triggered, reset the userClickedPattern to an empty array ready for the next level.
    userClickedPattern = [];
    // increase the level by 1 every time nextSequence() is called.
    level1++;

    // update the h1 with this change in the value of level.
    $("#level-title").text("Level "+level1);
    var randomnumber = Math.floor (Math.random()*4);
    var randomChoosenColour = buttonColours[randomnumber];
    gamePattern.push(randomChoosenColour);

    // Use jQuery to select the button with the same id as the randomChosenColour
    // Use jQuery to animate a flash to the button selected
    $("#"+randomChoosenColour).fadeIn(100).fadeOut(100).fadeIn(100);
    playsound(randomChoosenColour);
}

function playsound(name){
    // Play sound for the button colour selected
    var audio = new Audio('sounds/'+name+".mp3");
    audio.play();
}

function animatePress(currentColour){
    $("#"+currentColour).addClass("pressed");
    
    setTimeout(function(){
        $("#"+currentColour).removeClass("pressed");
    },100);
}

// Create a new function called checkAnswer(), it should take one input with the name currentLevel
function checkAnswer(currentLevel){
    // Write an if statement inside checkAnswer() to check if the most recent user answer is the same as the game pattern. If so then log "success", otherwise log "wrong".
    if(gamePattern[currentLevel] === userClickedPattern[currentLevel]){
        console.log("success");
        
        // If the user got the most recent answer right in step 3, then check that they have finished their sequence with another if statement.
        if(userClickedPattern.length == gamePattern.length){

            // Call nextSequence() after a 1000 millisecond delay.
            setTimeout(function(){
                nextSequence();
            },1000);
        }
    }
    if(gamePattern[currentLevel] != userClickedPattern[currentLevel]){
        console.log("Fail");
        
        var audio1 = new Audio('sounds/wrong.mp3');
        audio1.play();
        
        $("body").addClass("game-over");
        
        setTimeout(function(){
            $("body").removeClass("game-over");    
        },200);

        $("h1").text("Game Over, Press This to Restart");
        startOver();
    }
}

function startOver(){
    level1 = 0;
    gamePattern = [];
    started = true;
}