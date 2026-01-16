window.onload = function(){
    var img = document.getElementById("popyuki_notpop");
    var count = document.getElementById("score");
    var score = 0;
    var audio = new Audio("Nihaha.mp4");
    var bgm = document.getElementById("bgm");
    
    bgm.volume = 0.1;
    img.addEventListener('mousedown', function(){
        scoreUp();
        img.src = "images/popyuki2.png";
        audio.play();
    });

    img.addEventListener('mouseup', function(){
        img.src = 'images/popyuki1.webp';
    });

    function scoreUp(){
        score++;
        count.innerHTML = score;
    }
}