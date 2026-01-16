import React, { Component } from 'react';
import { View, WebView, StatusBar } from 'react-native';

export default class App extends Component {
    render() {

        var webViewCode = `
<html>
<head>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css" integrity="sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u" crossorigin="anonymous">
<script src="https://ajax.googleapis.com/ajax/libs/jquery/2.1.4/jquery.min.js"></script>
<script type="text/javascript" src="https://static.codehs.com/gulp/0c46c5da54af4d95de8cd2af3f1ae7c279106565/chs-js-lib/chs.js"></script>

<style>
    body, html {
        margin: 0;
        padding: 0;
    }
    canvas {
        margin: 0px;
        padding: 0px;
        display: inline-block;
        vertical-align: top;
    }
    #btn-container {
        text-align: center;
        padding-top: 10px;
    }
    #btn-play {
        background-color: #8cc63e;
    }
    #btn-stop {
        background-color: #de5844;
    }
    .glyphicon {
        margin-top: -3px;
        color: #FFFFFF;
    }
</style>
</head>

<body>
    <div id="canvas-container" style="margin: 0 auto; ">
        <canvas
        id="game"
        width="400"
        height="480"
        class="codehs-editor-canvas"
        style="width: 100%; height: 100%; margin: 0 auto;"
        ></canvas>
    </div>
    <div id="console"></div>
    <div id="btn-container">
        <button class="btn btn-main btn-lg" id="btn-play" onclick='stopProgram(); runProgram();'><span class="glyphicon glyphicon-play" aria-hidden="true"></span></button>
        <button class="btn btn-main btn-lg" id="btn-stop" onclick='stopProgram();'><span class="glyphicon glyphicon-stop" aria-hidden="true"></span></button>
    </div>

<script>
    var console = {};
    console.log = function(msg){
        $("#console").html($("#console").html() + "     " + msg);
    };

    var runProgram = function() {
        //Variables for making a big bee//
var head = new Circle(100);
var eye = new Circle(7);
var eyes = new Circle(7);

var body = new Arc(100,1,180,0);
var antenna = new Line(15,20,65,15);
var antennas = new Line(15,20,65,15);

var wing = new Oval(170,80);
var wings= new Oval(170,80);
var smallhead= new Circle(25);

var smalleye= new Circle(7);
var babyhead= new Circle(25);
var babybody= new Arc(25,1,180,0);

var babyeye= new Circle(3);
var babyeyes= new Circle(3);
var babywing= new Oval(10,30);
var babywings= new Oval(10,30);
//Variables for making "bees" move//
var hive;
var swarm;
var dx = 4;
var dy = 4;

//My Start Function//
function start(){
    firstJoke();
    secondJoke();
    background();
    bee();
    babyBee();
    cleverbee();
    yellowball();
    hives();
	
}
//My joke storyline//
function firstJoke(){
    var first = readLine("Directions: Figure out the pun. Are you ready? "); 
    var second = readLine("What bee is good for your health? ");
    if(second = bee){
		println("You got it right!");
    }
}

//This is a challenge joke//
function secondJoke(){
    var beginning= readLine("So... you got the joke, prepare for a challenge! Are you ready? ");
    var next= readLine("What are the cleverest bees? ");
    if(next = bee){
        println("Wow! Your smart");
    }
    var gift= readLine("Since you buzzed my puns, I will reward you. Are you ready? ");
}
//Creation of mama bee//
function bee(){
    head.setPosition(200,250);
    head.setColor(Color.yellow);
    add(head);
    
    eye.setPosition(150,200);
    eye.setColor(Color.black);
    add(eye);
 
    eyes.setPosition(250, 200);
    eyes.setColor(Color.black);
    add(eyes);
    
    body.setPosition(200,260);
    body.setColor(Color.black);
    body.rotate(180);
    add(body);
    
    antenna.move(160,120);
    antenna.setColor(Color.black);
    antenna.rotate(95);
    add(antenna);
    
    antennas.move(150,120);
    antennas.setColor(Color.black);
    antennas.rotate(95);
    add(antennas);

    wing.setPosition(70, 200);
    wing.rotate(55);
    wing.setColor(Color.gray);
    add(wing);
    
    wings.setPosition(330, 200);
    wings.rotate(118);
    wings.setColor(Color.gray);
    add(wings);
    
    smallhead.move(195,100);
    add(smallhead);
}
//Hehehe, it's from my joke (What are the cleverest bees?)//
function cleverbee(){
    var glasses = new WebImage("http://clipartix.com/wp-content/uploads/2016/04/Sunglasses-clipart-free-clip-art-clipartwiz.png");
    glasses.setSize(180,100);
    glasses.setPosition(getWidth()/3.8, getHeight()/3.2);
    add(glasses);
}
//Creation of baby bee//
function babyBee(){
    babyhead.setPosition(200,420);
    babyhead.setColor(Color.yellow);
    add(babyhead);
    
    babybody.setPosition(200,420);
    babybody.rotate(180);
    babybody.setColor(Color.black);
    add(babybody);
    
    babyeye.setPosition(195, 410);
    babyeye.setColor(Color.black);
    add(babyeye);
    
    babyeyes.setPosition(205, 410);
    babyeyes.setColor(Color.black);
    add(babyeyes);
    
    babywing.setPosition(230,410);
    babywing.rotate(50);
    babywing.setColor(Color.gray);
    add(babywing);
    
    babywings.setPosition(170, 408);
    babywings.rotate(-28);
    babywings.setColor(Color.gray);
    add(babywings);
}
//Orange color background//
function background(){
    setBackgroundColor(Color.orange);
}
//One of the flying bees//
function yellowball(){
    hive = new Circle(40);
	hive.setPosition(250, 100);
	add(hive);
	mouseClickMethod(making);
	hive.setColor(Color.yellow);
	setTimer(making, 10);
}
//Another flying bee//
function hives(){
    swarm = new Circle(40);
	swarm.setPosition(200, 300);
	add(swarm);
	mouseClickMethod(production);
	swarm.setColor(Color.yellow);
	setTimer(production, 15);
}
//How they move//
function production(){
    barrier();
    swarm.move(dx, dy);
}
//How they move//
function making(){
    walls();
	hive.move(dx, dy);
}
//Bouncing of the walls//
function walls(){
	// Bounce off right wall
	if(hive.getX() + hive.getRadius() > getWidth()){
		dx = -dx;
	}
	
	// Bounce off left wall
	if(hive.getX() - hive.getRadius() < 0){
		dx = -dx;
	}
	
	// Bounce off bottom wall
	if(hive.getY() + hive.getRadius() > getHeight()){
		dy = -dy;
	}
	
	// Bounce off top wall
	if(hive.getY() - hive.getRadius() < 0){
		dy = -dy;
	}
}
//Bouncing of the walls//
function barrier(){
    // Bounce off right wall
	if(swarm.getX() + swarm.getRadius() > getWidth()){
		dx = -dx;
	}
	
	// Bounce off left wall
	if(swarm.getX() - swarm.getRadius() < 0){
		dx = -dx;
	}
	
	// Bounce off bottom wall
	if(swarm.getY() + swarm.getRadius() > getHeight()){
		dy = -dy;
	}
	
	// Bounce off top wall
	if(swarm.getY() - swarm.getRadius() < 0){
		dy = -dy;
	}
}



        if (typeof start === 'function') {
            start();
        }

        // Overrides setSize() if called from the user's code. Needed because
        // we have to change the canvas size and attributes to reflect the
        // user's desired program size. Calling setSize() from user code only
        // has an effect if Fit to Full Screen is Off. If Fit to Full Screen is
        // On, then setSize() does nothing.
        function setSize(width, height) {
            if (!true) {
                // Call original graphics setSize()
                window.__graphics__.setSize(width, height);

                // Scale to screen width but keep aspect ratio of program
                // Subtract 2 to allow for border
                var canvasWidth = window.innerWidth - 2;
                var canvasHeight = canvasWidth * getHeight() / getWidth();

                // Make canvas reflect desired size set
                adjustMarginTop(canvasHeight);
                setCanvasContainerSize(canvasWidth, canvasHeight);
                setCanvasAttributes(canvasWidth, canvasHeight);
            }
        }
    };

    var stopProgram = function() {
        removeAll();
        window.__graphics__.fullReset();
    }

    window.onload = function() {
        if (!false) {
            $('#btn-container').remove();
        }

        var canvasWidth;
        var canvasHeight;
        if (true) {
            // Get device window width and set program size to those dimensions
            setSize(window.innerWidth, window.innerHeight);
            canvasWidth = getWidth();
            canvasHeight = getHeight();

            if (false) {
                // Make room for buttons if being shown
                $('#btn-container').css('padding', '5px 0');
                canvasHeight -= $('#btn-container').outerHeight();
            }

            setCanvasAttributes(canvasWidth, canvasHeight);
        } else {
            // Scale to screen width but keep aspect ratio of program
            // Subtract 2 to allow for border
            canvasWidth = window.innerWidth - 2;
            canvasHeight = canvasWidth * getHeight() / getWidth();

            // Light border around canvas if not full screen
            $('#canvas-container').css('border', '1px solid #beccd4');

            adjustMarginTop(canvasHeight);
        }

        setCanvasContainerSize(canvasWidth, canvasHeight);

        if (true) {
            runProgram();
        }
    };

    // Set the canvas container width and height.
    function setCanvasContainerSize(width, height) {
        $('#canvas-container').width(width);
        $('#canvas-container').height(height);
    }

    // Set the width and height attributes of the canvas. Allows
    // getTouchCoordinates to sense x and y correctly.
    function setCanvasAttributes(canvasWidth, canvasHeight) {
        $('#game').attr('width', canvasWidth);
        $('#game').attr('height', canvasHeight);
    }

    // Assumes the Fit to Full Screen setting is Off. Adjusts the top margin
    // depending on the Show Play/Stop Buttons setting.
    function adjustMarginTop(canvasHeight) {
        var marginTop = (window.innerHeight - canvasHeight)/2;
        if (false) {
            marginTop -= $('#btn-container').height()/3;
        }
        $('#canvas-container').css('margin-top', marginTop);
    }
</script>
</body>
</html>
`;
        return (
            <View style={{ flex: 1 }}>
                <StatusBar hidden />
                <WebView
                    source={{html: webViewCode, baseUrl: "/"}}
                    javaScriptEnabled={true}
                    style={{ flex: 1 }}
                    scrollEnabled={false}
                    bounces={false}
                    scalesPageToFit={false}
                ></WebView>
            </View>
        );
    }
}