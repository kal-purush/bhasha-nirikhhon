$(document).ready(function(){
    var wins = 0
    var losses = 0 

    var redGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1 
    var blueGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1  
    var greenGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1  
    var yellowGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1 
    
    var totalScore = 0
    var computerChoice = Math.floor(Math.random() * (120 - 19 + 1)) + 19  

    document.getElementById("user-score").innerHTML = "Total Score: " + "<br>" + totalScore;
    document.getElementById("totals").innerHTML = "Wins: " + wins + " " + "Losses: " + losses;
    document.getElementById("computer-choice").innerHTML = computerChoice;

    $("#blueGem").on("click", function() {
        
        document.getElementById("user-score").innerHTML = "Total Score: " + "<br>" + myFunction(blueGem);
        
        function myFunction(a) {

            totalScore += a;

            if (totalScore < computerChoice) {
                return totalScore; 
            }  
            
            else if (totalScore > computerChoice) {
                alert("You Lose!");
                losses ++; 
                blueGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;
                redGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                greenGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                yellowGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;    
                computerChoice = Math.floor(Math.random() * (120 - 19 + 1)) + 19;
                totalScore = 0;
            }  
            
            else if (totalScore === computerChoice) {
                alert("You Win!");
                wins ++;
                totalScore = 0;
                blueGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;
                redGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                greenGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                yellowGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;
                computerChoice = Math.floor(Math.random() * (120 - 19 + 1)) + 19;
            }

            document.getElementById("totals").innerHTML = "Wins: " + wins + " " + "Losses: " + losses;
            document.getElementById("computer-choice").innerHTML = computerChoice;
            document.getElementById("user-score").innerHTML = "Total Score: " + "<br>" + totalScore;
        }

    });

    $("#redGem").on("click", function() {

        document.getElementById("user-score").innerHTML = "Total Score: " + "<br>" + myFunction(redGem);
        
        function myFunction(a) {

            totalScore += a;

            if (totalScore < computerChoice) {
                return totalScore; 
            }  
            
            else if (totalScore > computerChoice) {
                alert("You Lose!");
                losses ++; 
                blueGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;
                redGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                greenGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                yellowGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1; 
                computerChoice = Math.floor(Math.random() * (120 - 19 + 1)) + 19;
                totalScore = 0;
            }  
            
            else if (totalScore === computerChoice) {
                alert("You Win!");
                wins ++;
                totalScore = 0;
                blueGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;
                redGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                greenGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                yellowGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                computerChoice = Math.floor(Math.random() * (120 - 19 + 1)) + 19;
            }

            document.getElementById("totals").innerHTML = "Wins: " + wins + " " + "Losses: " + losses;
            document.getElementById("computer-choice").innerHTML = computerChoice;
            document.getElementById("user-score").innerHTML = "Total Score: " + "<br>" + totalScore;
        }

    });

    $("#yellowGem").on("click", function() {

       document.getElementById("user-score").innerHTML = "Total Score: " + "<br>" + myFunction(yellowGem);
        
        function myFunction(a) {

            totalScore += a;

            if (totalScore < computerChoice) {
                return totalScore; 
            }  
            
            else if (totalScore > computerChoice) {
                alert("You Lose!");
                losses ++; 
                blueGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;
                redGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                greenGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                yellowGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                computerChoice = Math.floor(Math.random() * (120 - 19 + 1)) + 19;
                totalScore = 0;
            }  
            
            else if (totalScore === computerChoice) {
                alert("You Win!");
                wins ++;
                totalScore = 0;
                blueGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;
                redGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                greenGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                yellowGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                computerChoice = Math.floor(Math.random() * (120 - 19 + 1)) + 19;
            }

            document.getElementById("totals").innerHTML = "Wins: " + wins + " " + "Losses: " + losses;
            document.getElementById("computer-choice").innerHTML = computerChoice;
            document.getElementById("user-score").innerHTML = "Total Score: " + "<br>" + totalScore;
        }

    });

    $("#greenGem").on("click", function() {

        document.getElementById("user-score").innerHTML = "Total Score: " + "<br>" + myFunction(greenGem);
        
        function myFunction(a) {

            totalScore += a;

            if (totalScore < computerChoice) {
                return totalScore; 
            }  
            
            else if (totalScore > computerChoice) {
                alert("You Lose!");
                losses ++; 
                blueGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;
                redGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                greenGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                yellowGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                computerChoice = Math.floor(Math.random() * (120 - 19 + 1)) + 19;
                totalScore = 0;
            }  
            
            else if (totalScore === computerChoice) {
                alert("You Win!");
                wins ++;
                totalScore = 0;
                blueGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;
                redGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                greenGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                yellowGem = Math.floor(Math.random() * (12 - 1 + 1)) + 1;  
                computerChoice = Math.floor(Math.random() * (120 - 19 + 1)) + 19;
            }

            document.getElementById("totals").innerHTML = "Wins: " + wins + " " + "Losses: " + losses;
            document.getElementById("computer-choice").innerHTML = computerChoice;
            document.getElementById("user-score").innerHTML = "Total Score: " + "<br>" + totalScore;
        }
    });

    
});


 
