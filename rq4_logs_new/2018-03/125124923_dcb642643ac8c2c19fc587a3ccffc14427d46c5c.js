var psychicLetters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'];
        var Win = 0;
        var Lose = 0;
        var guessesLeft = 9;
        var guessedLetters = []; //Array to capture user letter input

        document.onkeyup = function (event) {
            var yourGuess = event.key; //variable to capute user's guess
            guessedLetters.push(yourGuess); //This is suppose to capture letters entered by the user    
            var psychicAns = psychicLetters[Math.floor(Math.random() * psychicLetters.length)];

            //If your guess matches the psychic's guess,
            //a point is added to the "Wins" tally and the guesses tally restarts.
            if (yourGuess === psychicAns) {
                Win++;
                guessesLeft = 9;
                alert("Bingo You win!!!");
            }
            //If your guess does not match the psychic's guess, your number of  guesses remaining are deducted.
            else {
                guessesLeft--;
                //You can add alert if you want to be notified everytime you Guessed wrong
                //alert("Wrong Guess");
            }

            //If the guesses remaining equals 0, you lose, an increment is added to  the "Losses" tally and the points restart.
            if (guessesLeft === 0) {
                Lose++;
                guessesLeft = 9;
                alert("Last Guess Left")
            }

            var html = "<h1>The Psychic Game</h1>" + "<p>Guess what letter I'm thinking of:</p>" + "<p>Wins: " + Win + "</p>" + "<p>Losses: " + Lose + "<p>Guesses Left: " + guessesLeft + "<p>Your Guesses So Far: " + guessedLetters.join(', '); //Show guessed letters here
            document.querySelector("#psychicGame").innerHTML = html;
        }