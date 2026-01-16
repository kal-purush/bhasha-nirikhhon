//Begin game when page is loaded
$(document).ready(function(){
    
//declaring variables
    var winCount=0;                     //win count starts at 0
    var lossCount=0;                    //loss count starts at 0
    var randNumber;                     //random target number
    var userGuess=0;                    //users total guess
    var winStreak=0;                    //total wins in a row
    var lossStreak=0;                   //total losses in a row

//declaring functions//



    //reset function
    function reset(){                                   
        randNumber = Math.floor(Math.random()*101+19);
        crystal1 = Math.floor(Math.random() *11+1);
        crystal2 = Math.floor(Math.random() *11+1);
        crystal3 = Math.floor(Math.random() *11+1);
        crystal4 = Math.floor(Math.random() *11+1);
        console.log(crystal1 + " " +crystal2 + " " +crystal3 + " " +crystal4);
        userGuess = 0;
        updateDisplay();}

    //update displayed number function
    function updateDisplay(){
        $("#userTotal").text(userGuess);
        $('#randomTargetNumber').text(randNumber);
        $('#randomTotalMinus').text(randNumber - userGuess);
        $('#losses').text(lossCount);
        $('#wins').text(winCount);
        if(winStreak  > 0){
            $('#winStreak').text(winStreak);
            $('#lossStreak').text("");}
        if(lossStreak > 0){
            $("#winStreak").text("");
            $("#lossStreak").text(lossStreak);}}

    //winGame function
    function winGame(){
        winCount++;
        $('#wins').text(winCount);
        winStreak++;
        lossStreak = 0;
        reset();}

    //lossGame function
    function lossGame(){
        lossCount++;
        $('#losses').text(lossCount);
        winStreak = 0;
        lossStreak++;
        reset();}
    
    reset();                            //run reset when game loads
    updateDisplay();                    //run update display when game loads
    

//Button Controls (When Clicked)//

    $("#button1").on ('click', function(){
        userGuess += crystal1;
        //console.log(userGuess);
        updateDisplay();
       if (userGuess == randNumber){
            winGame();
        }
        if (userGuess > randNumber){
           lossGame();
        }
    })
    
    $("#button2").on ('click', function(){
        userGuess += crystal2;
        //console.log(userGuess);
        updateDisplay();
       if (userGuess == randNumber){
            winGame();
        }
        if (userGuess > randNumber){
           lossGame();
        }
    })

    $("#button3").on ('click', function(){
        userGuess += crystal3;
        //console.log(userGuess);
        updateDisplay();
       if (userGuess == randNumber){
            winGame();
        }
        if (userGuess > randNumber){
           lossGame();
        }
    })

    $("#button4").on ('click', function(){
        userGuess += crystal4;
        //console.log(userGuess);
        updateDisplay();
       if (userGuess == randNumber){
            winGame();
        }
        if (userGuess > randNumber){
           lossGame();
        }
    })

});