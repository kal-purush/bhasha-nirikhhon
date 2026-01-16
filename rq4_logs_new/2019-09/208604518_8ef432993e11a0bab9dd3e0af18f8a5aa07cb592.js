
function drawEventCard() {
  if (players == 0) {
    getPlayerNames();
  }
  document.getElementById("event").innerHTML = "";
  if (numbers == 0) {
    document.getElementById("new").style = "display:block;";
    shuffleEventCards(numbers, events);
    document.getElementById("nextPlayer").innerHTML = "Next: " + players[playerCounter];
  } else { 
    document.getElementById("new").style = "display:none;";
    if (players.length - 1 < playerCounter) {
        playerCounter = 0;
    }
    if(players[playerCounter+1] == undefined){
      document.getElementById("nextPlayer").innerHTML = "Next: " + players[0];
    }else{
      document.getElementById("nextPlayer").innerHTML = "Next: " + players[playerCounter + 1];
    }
    document.getElementById("displayPlayerName").innerHTML = players[playerCounter] + "'s turn";
    playerCounter++;
    if (rollNumber(numbers) == 7) {
      document.getElementById("event").innerHTML +=
        "Robber attacks<br>1. Each player with more  than 7 cards must discard half (rounded down).<br>2. Move the robber. Draw a random resource card from any 1 player with a settlement and/or city next to the robber's new hex.";
    } else {
      rollEvent(events);
    }
  }
}

function startGame() {
  numbers = [];
  events = [];
  players = [];
  playerCounter = 0;
  getPlayerNames();
  if(numberOfPlayers == players.length){
    document.getElementById("displayPlayerName").innerHTML = "";
    if(validatePlayerNames() > 0){
      alert("Please enter valid player names");
    }else{
      rollForInitiative();
      drawEventCard();
      document.getElementById("playerNames").style = "display: none";
      document.getElementById("cardsInterface").style = "display: block";
    }
  }else{
    alert("Please enter player names");
  }
}

function newGame() {
  document.getElementById("cardsInterface").style = "display:none;";
  document.getElementById("playerNames").style = "display: block;";
  document.getElementById("Player1").value = "";
  document.getElementById("Player2").value = "";
  document.getElementById("Player3").value = "";
  document.getElementById("Player4").value = "";
  document.getElementById("nextPlayer").innerHTML = "";
}