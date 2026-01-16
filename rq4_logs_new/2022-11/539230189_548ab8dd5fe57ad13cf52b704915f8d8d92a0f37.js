var canvas = document.getElementById("myCanvas");
var ctx = canvas.getContext("2d");

var request = "";
var diameter = 50;
var cirColor = "black";

// draw canvas
drawCircle();
function drawCircle() {
    ctx.beginPath();
    ctx.rect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = "white";
    ctx.fill();

    ctx.beginPath();
    ctx.arc(canvas.width * 0.5, canvas.height * 0.5, 50 / 2, 0, 2 * Math.PI);
    ctx.stroke();
    ctx.fillStyle = "black";
    ctx.fill();
}

var button = document.getElementById("button");
// Function for change speak button and stop button for voice recogition 
function speak() {
    var button = document.getElementById("button");
    if (button.innerHTML == "Speak") {
        speak.play();
        button.innerHTML = "Stop";
        recognition.start();
    } else {
        stop.play();
        button.innerHTML = "Speak";
        recognition.stop();
    }
}

var recognition = new webkitSpeechRecognition();
    recognition.continuous = false;
    recognition.interimResults = true;
    color = "Red Orange Yellow Green Blue Purple White Black "
 
    recognition.onresult = function (e) {
        var button = document.getElementById("button");
        for (var i = e.resultIndex; i < e.results.length; ++i) {
            if (e.results[i].isFinal) {
                button.value += e.results[i][0].transcript;
                stop.play();
                button.innerHTML = "Speak";
                recognition.stop();
                Response();
            }
        }
    }

function Response(){
    var key = request.split(" ")[0];
    var option = request.split(" ")[1];

    if(key.localeCompare("color") == 0){
        cirColor = option;
        if(!isNaN(color.match(/option/))){
            var msg = new SpeechSynthesisUtterance("Change color to " + request);
            setTimeout(function () { window.speechSynthesis.speak(msg); }, 1000);
            drawCircle();
        }else{
            var msg = new SpeechSynthesisUtterance("Try again, did not recognize" );
            setTimeout(function () { window.speechSynthesis.speak(msg); }, 1000);
        }
    }else if(key.localeCompare("Size") == 0){
        if (!isNaN(option)) {
            diameter = parseInt(option);
            if (diameter > 300) {
                var msg = new SpeechSynthesisUtterance("Size too big , size limit 300");
                setTimeout(function () { window.speechSynthesis.speak(msg); }, 1000);
            }
            else if (diameter < 1) {
                var msg = new SpeechSynthesisUtterance("Size too small, the minimize size is 1");
                setTimeout(function () { window.speechSynthesis.speak(msg); }, 1000);
            }
            else {
                var msg = new SpeechSynthesisUtterance("Change size to " + request);
                setTimeout(function () { window.speechSynthesis.speak(msg); }, 1000);
                drawCircle();
            }
        }
        else {
            var msg = new SpeechSynthesisUtterance("Try again, did not recognize")
            setTimeout(function () { window.speechSynthesis.speak(msg); }, 1000);
        }
    }else if (key.localeCompare("help") == 0) {
        var msg = new SpeechSynthesisUtterance("Here is the instructions for you. Say color, followed by a color, to set the circle color. Say size, followed of a number from 1 to 300, to set the diameter of the circle");
        setTimeout(function () { window.speechSynthesis.speak(msg); }, 1000);
    }
    console.log(request);
    request = "";
}