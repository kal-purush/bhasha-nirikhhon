$(document).ready(function () {

    function Character(name, hp, attack, attackBack, ) {
        this.name = name;
        this.hp = hp;
        this.attack = attack;
        this.attackBack = attackBack;
    }

    var myImage;

    $("#player1").click(function () {
        playerCharacter = new Character('Cloud Strife', 120, 8, 15);
    })

    $("#player2").click(function () {
        playerCharacter = new Character('Tifa Lockhart', 100, 14, 5);
    })

    $("#player3").click(function () {
        playerCharacter = new Character('Vincent Valentine', 150, 8, 20);

    })

    $("#player4").click(function () {
        playerCharacter = new Character('Sephiroth', 180, 7, 20);
    })

    let myCharacter;

    $("#player1").on('click', function () {
        if (myCharacter) {
            $("#enemy img")[0].src = $("#player1 img")[0].src;
            $("#player1")[0].remove();
        } else {
            myCharacter = "Player 1"
            $("#character img")[0].src = $("#player1 img")[0].src;
            $("#player1")[0].remove();
            enemySelect();
        }
    })
    $("#player2").on('click', function () {
        if (myCharacter) {
            $("#enemy img")[0].src = $("#player2 img")[0].src;
            $("#player2")[0].remove();
        } else {
            myCharacter = "Player 2"
            $("#character img")[0].src = $("#player2 img")[0].src;
            $("#player2")[0].remove();
            enemySelect();
        }
    })
    $("#player3").on('click', function () {
        if (myCharacter) {
            $("#enemy img")[0].src = $("#player3 img")[0].src;
            $("#player3")[0].remove();
        } else {
            myCharacter = "Player 3"
            $("#character img")[0].src = $("#player3 img")[0].src;
            $("#player3")[0].remove();
            enemySelect();
        }
    })
    $("#player4").on('click', function () {
        if (myCharacter) {
            $("#enemy img")[0].src = $("#player4 img")[0].src;
            $("#player4")[0].remove();
        } else {
            myCharacter = "Player 4"
            $("#character img")[0].src = $("#player4 img")[0].src;
            $("#player4")[0].remove();
            enemySelect();
        }
    })

    const enemySelect = () => {
        $('#gallery').appendTo("#enemySelect")
    }

    //attack function linked to button id

    $('#attack').click(function () {
        playerCharacter.attack
    })


})

var turn = 1;


// link the images with the values here



// determine health loss for player and enemy

// win con

// loss con