// When a user first visits the webpage: 
//  The document loads.
$(document).ready(function(){    //My JS starts past this point.

// Initialize Firebase
var config = {
    apiKey: "AIzaSyAqycBI1YOI5dZvzDnbiiDLItRcbe3h7qo",
    authDomain: "unit-7-rock-paper-scissors.firebaseapp.com",
    databaseURL: "https://unit-7-rock-paper-scissors.firebaseio.com",
    projectId: "unit-7-rock-paper-scissors",
    storageBucket: "unit-7-rock-paper-scissors.appspot.com",
    messagingSenderId: "55092492997"
};
firebase.initializeApp(config);

//Firebase Variables
    const database = firebase.database(); //A variable that refers directly to the database.
    const allUsers = database.ref("/allUsers"); //A variable that refers to the All Players directory.
    const allGameRooms = database.ref("/allGameRooms"); //A variable that refers to the All Game Rooms directory.
    let playerKey; // Establish a variable that hold's this player's profile's push key.
    let myDir = "/allUsers/" + playerKey; //Establish a variable that holds this player's direct directory path on Firebase.
    const myDirRef = database.ref(myDir); // This variable can be used to directly reference this player's directory.
    let myRoomKey; //Establish a variable that hold this player's game room's push key.
    const myRoom = "/allGameRooms/" + myRoomKey; // Establish a variable that hold this player's room's direct directory path on Firebase.
    const myRoomRef = database.ref(myRoom);  // This variable can be used to directly reference this player's room's directory.
    const connectedRef = database.ref(".info/connected"); //This Variable refers directly to the information reference that tells us if we are connected or not.

    let playerName;

    // const currentEmptyRoom = database.ref("/currentEmptyRoom");
    // const emptyRoomId = database.ref("/currentEmptyRoom/emptyRoomId");
    // let userNum = database.ref("/userNum");
    // let deepUserNum = database.ref("/userNum/0");
    // let userTicker = 0;
    let thisPlayer;

    let myGameRoom;
    let emptyRoom;


//Global Variables
let totalPlayers = 0;
const player1Name = $(".player1");
const player2Name = $(".player2");
intervalId = 0;


//Game Functions
gameFunctions = {
    firebaseListeners: function() {

        connectedRef.on("value", function(conSnap){ 
            console.log("I'm Connected!")
        }, function(errorObject) {
            console.log("The read failed: " + errorObject.code);
        });

        myRoomRef.ref(".roomFull").on("value", function(roomFullSnap){ 
            console.log("Checking If the Room is Full!");
            if(roomFullSnap.val()){
                console.log("The Room is full!")
            } else {
                console.log("The Room isn't full!")
            }
        }, function(errorObject) {
            console.log("The read failed: " + errorObject.code);
        });

    },
    userConnect: function(){
        if(gameOn && totalPlayers === 0) {
            name = $(".playerNameInput").val().trim();
            allUsers.child(playerKey).update({
                name: name,
                nameAdded: true,
                firstEntry: true,
            });
        } else if (gameOn && totalPlayers === 1) {
            name = $(".playerNameInput").val().trim();
            allUsers.child(playerKey).update({
                name: name,
                nameAdded: true,
            });
        }
    },
    createGameRoom: function(){ // This function creates a new game room.
        makeRoom = allGameRooms.push({ //Create a push that sends all game state data for this game room to Firebase.
            gameRoomId: "",
            startScreen: true,
            gameOn: false,
            playerReady: false,
            opponentReady: false,
            gameOver: false,
            player1Id: "",
            player1Choice: "",
            player1Chose: false,
            player2Id: "",
            player2Choice: "",
            player2Chose: false,
            clockRunning: false,
        })
        myGameRoom = makeRoom.key; //Grab the unique key of the game room's push.
        pushRoomId(); //Call the function that will now use the key that was just grabbed.
        function pushRoomId(){
            allGameRooms.child(myGameRoom).update({ // Target the game room we just created and then update its gameRoom Id property with its key.
                gameRoomId: myGameRoom, 
            })
            currentEmptyRoom.set({ //If there are no old Empty Room Ids, establish the empty key.
                emptyRoomId: "",
            })
            currentEmptyRoom.update({ //Update the key with the current room. 
                emptyRoomId: myGameRoom,
            })
        }
    },
    kickPlayer: function(){
        if(allUsers.child(playerKey).playerNumber === 1){
            allGameRooms.child(myGameRoom).update({
                player1Id: "",
            })
        } else{
            if (allUsers.child(playerKey).playerNumber === 2){
                allGameRooms.child(myGameRoom).update({
                    player2Id: "",
                })
            } 
        }
    },
    createNewPlayer: function(){ //This function creates a new player.
        thisPlayer = allUsers.push({ //create and capture a push to Firebase with the new player data.
            name: "",
            wins: 0,
            loses: 0,
            playerPick: "",
            playerNumber: 0,
            nameAdded: false,
            connected: true,
            firstEntry: false,
            playerReady: false,
            playerChoiceMade: false,
            hasOpponent: false,
            opponentId: "",
            gameRoomId: "",
        });
        thisPlayer.onDisconnect().remove(); //If this player disconnects, remove them from Firebase.
        playerKey = thisPlayer.key; //Grab the key of the push we just made.
        userTicker++ //Increment the User Ticker
        deepUserNum.update({ //Send the new value up to the Firebase user counter.
            userTicker: userTicker
        })
    },
    closeEmptyRooms: function(){
        const allRooms = allGameRooms.orderByKey();
        allRooms.once("value")
        .then(function(snapshot){
            snapshot.forEach(function(childSnap){
                let childProps = childSnap.val();
                if (childProps.player1Id === "" && childProps.player2Id === ""){
                    allGameRooms.child(childProps.gameRoomId).remove();
                }
            })             
        })
    }
};

//Constructors and Prototypes 
function sound(src) { //This makes the sound objects that are used when cards are made.
    this.sound = document.createElement("audio");
    this.sound.src = src;
    this.sound.setAttribute("preload", "auto");
    this.sound.setAttribute("controls", "none");
    this.sound.style.display = "none";
    document.body.appendChild(this.sound);
    this.play = function(){
        this.sound.play();
    }
    this.stop = function(){
        this.sound.pause();
    }
} 

//Click Events
$(".pNameBtn").click( function(event){ //When the player clicks the button to submit their display-name.
    event.preventDefault();
    playerName

});

$(".startPlayBtn").click( function(event){//When the Player hits the start screen polay button.
    event.preventDefault();
    gameOn = true;
    gameFunctions.userConnect();
});

$(".rpsSelectopn").click( function(event){//When the player selects either Rock, Paper or Scissors by clicking on them.
    event.preventDefault();

});

$(".playAgainBtn").click( function(event){//When the player clicks the Play Again Button.
    event.preventDefault();

});

$(".sayBtn").click( function(event){//When the Player submits text to the chat.
    event.preventDefault();

});

//Call database Listeners
gameFunctions.firebaseListeners();




//My JS Ends beyond this point.
});