$(function(){

$(".rules").click(function(){
    clearInterval(timer);
    stopWolfAnimation();
    $(".rule").stop().fadeIn(100);
})

$(".close").click(function(){
    $(".progress").css({
        width: 180
    })
    $(".rule").stop().fadeOut(100);
    $(".start").stop().fadeIn(100);
})

$(".start").click(function(){

    $(this).stop().fadeOut(100);
    progressHandler();
    startWolfAnimation();
})

$(".end").click(function(){
    clearInterval(timer);
    stopWolfAnimation();
    $(".progress").css({
        width:180
    })
    $(".score").text(0);
    $(".end").stop().fadeOut(100);
    $(".start").stop().fadeIn(100);
})

$(".restart").click(function(){

    $(".mask").stop().fadeOut(100);
    progressHandler();
    startWolfAnimation();
    $(".score").text(0);
})

$(".again").click(function(){
    $(".score").text(0);
    $(".success").stop().fadeOut(100);
    progressHandler();
    startWolfAnimation();
})

var timer;
function progressHandler(){
    $(".progress").css({
        width: 180
    })

    timer= setInterval(function(){
        var progressWidth= $(".progress").width();
        progressWidth -= 1;

        $(".progress").css({
            width: progressWidth
        });

        if(progressWidth <= 0){
            clearInterval(timer);
            $(".mask").stop().fadeIn(100);
            $(".end").stop().fadeOut(100);
            stopWolfAnimation();
        }
    },150)
}

var wolfTimer;
function startWolfAnimation(){
    $(".end").stop().fadeIn(100);
    var wolf_1=['./images/h0.png','./images/h1.png','./images/h2.png','./images/h3.png','./images/h4.png','./images/h5.png','./images/h6.png','./images/h7.png','./images/h8.png','./images/h9.png'];
    var wolf_2=['./images/x0.png','./images/x1.png','./images/x2.png','./images/x3.png','./images/x4.png','./images/x5.png','./images/x6.png','./images/x7.png','./images/x8.png','./images/x9.png'];
    var arrPos=[
    {left:"100px",top:"115px"},
    {left:"20px",top:"160px"},
    {left:"190px",top:"142px"},
    {left:"105px",top:"193px"},
    {left:"19px",top:"221px"},
    {left:"202px",top:"212px"},
    {left:"120px",top:"275px"},
    {left:"30px",top:"295px"},
    {left:"209px",top:"297px"}
    ];

    var $wolfImage = $("<img src='' class='wolfImage'>");
    var posIndex = Math.round(Math.random() * 8);
    $wolfImage.css({
        position:"absolute",
        left:arrPos[posIndex].left,
        top:arrPos[posIndex].top
    });

    var wolfType = Math.round(Math.random()) == 0 ? wolf_1 : wolf_2;
    window.wolfIndex = 0;
    window.wolfIndexEnd = 5;

    wolfTimer = setInterval(function(){
        if(wolfIndex > wolfIndexEnd){
            // $wolfImage.remove();
            // clearInterval(wolfTimer);
            stopWolfAnimation();
            startWolfAnimation();
        }
        $wolfImage.attr("src",wolfType[wolfIndex]);
        wolfIndex++;
    },200)
    $(".container").append($wolfImage);

    gameRules($wolfImage);

    if($(".score").text() >= 70){
        clearInterval(timer);
        stopWolfAnimation();
        $(".success").stop().fadeIn(100);
        $(".end").stop().fadeOut(100);

    }
}

function gameRules($wolfImage){
    $wolfImage.one("click",function(){
        window.wolfIndex = 5;
        window.wolfIndexEnd = 9;
        var $src = $(this).attr("src");
        var flag = $src.indexOf("h") >= 0;

        if(flag){
        $(".score").text(parseInt($(".score").text())+10);
        }else{
        $(".score").text(parseInt($(".score").text())-10)};


    })
}

function stopWolfAnimation(){
    $(".wolfImage").remove();
    clearInterval(wolfTimer);
}

})