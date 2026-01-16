prediction_1="";
prediction_2="";

Webcam.set({
    width: 350,
    height: 300,
    image_format: 'png',
    png_quality: 90
});

camera=document.getElementById("camera");

Webcam.attach('#camera');

function take_snapshot(){
    Webcam.snap(function(data_uri){
        document.getElementById("result").innerHTML= '<img id="captured_image" src="'+data_uri+'"/></img>';
    
    });
}

console.log('ml5 version:', ml5.version);
classifier = ml5.imageClassifier('https://storage.googleapis.com/tm-model/L-8Uj7yRW/model.json');

function modelLoaded(){
    cosnole.log('Model Loaded');
}

function speak(){
    var synth = window.speechSynthesis;
    speak_data_1 = "The prediction is "+ prediction_1;
    var utterThis= new SpeechSynthesisUtterance(speak_data_1 + speak_data_2);
    synth.speak(utterThis);
}