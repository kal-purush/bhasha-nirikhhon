var yeah = "/YEEEEAAAAAAAAHH.mp3";
var audioElement = null;
$(function(){
    audioElement = document.createElement('audio');
    audioElement.setAttribute('src', yeah);
    $("#yeah").click(function(){
        start();
    });
    
});
$(window).keydown(function(e){
    if(e.keyCode == 13){
        start();   
    }
});


var yeahWords = "YEEEEEEEEEEEEEEEEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH";
var letter = 0;

function yeahh(){
    setTimeout(function(){
        $("#content").text($("#content").text() + yeahWords[letter]);
        letter++;
        if(letter < yeahWords.length){
            yeahh();
        }   
    }, 20);
}

function start(){
    $("#content").html("");
    yeahh();
    play();
}

function play(){
    audioElement.play();
}