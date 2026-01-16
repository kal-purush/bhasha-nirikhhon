
class Player {
    constructor(name) {
      this.name = name;
      this.score = 0;
      this.moves = [];
    }
  }

$(document).ready(function () {
    $("#submit-names").on("click", function () {
        var playerOneName = document.getElementById("player-one");
        console.log(playerOneName.value);
        var player = new Player(playerOneName.value);
        console.log(player.name);
        console.log(player.score);
        console.log(player.moves);
    });
});

function myFunction(){
    var playerOneName = document.getElementById("player-one");
    console.log(playerOneName.value);
    var player = new Player(playerOneName.value);
    console.log(player.name);
    console.log(player.score);
    console.log(player.moves);
}

function getFormInput(){
    var input1 = $('#player-one :input');
    var input2 = $('#player-two :input');
    var inputs = []
    inputs.push(input1, input2)
    console.log(inputs)
}

function test(){
    var player = new Player("brian");
    console.log(player.name);
    console.log(player.score);
    console.log(player.moves);
}