console.log("loaded");
var clap = document.getElementById('clap');
var hithat = document.getElementById('hithat');
var kick = document.getElementById('kick');
var openhat = document.getElementById('openhat');
var boom = document.getElementById('boom');
var ride = document.getElementById('ride');
var snare = document.getElementById('snare');
var tom = document.getElementById('tom');
var tink = document.getElementById('tink');
var iconss = document.getElementsByClassName('icons');

document.addEventListener('keydown',function(e){
    var keyEvent = e.key;
    switch(keyEvent)
    {
        case 'a':
            clap.play();
            iconss[0].style.boxShadow = "0px 0px 10px yellow";
            iconss[0].style.transform = "scale(1.1)";
            break;
        case 's':
            hithat.play();
            iconss[1].style.boxShadow = "0px 0px 10px yellow";
            iconss[1].style.transform = "scale(1.1)";
            break;
        case 'd':
            kick.play();
            iconss[2].style.boxShadow = "0px 0px 10px yellow";
            iconss[2].style.transform = "scale(1.1)";
            break;
        case 'f':
            openhat.play();
            iconss[3].style.boxShadow = "0px 0px 10px yellow";
            iconss[3].style.transform = "scale(1.1)";
            break;
        case 'g':
            boom.play();
            iconss[4].style.boxShadow = "0px 0px 10px yellow";
            iconss[4].style.transform = "scale(1.1)";
            break;
        case 'h':
            ride.play();
            iconss[5].style.boxShadow = "0px 0px 10px yellow";
            iconss[5].style.transform = "scale(1.1)";
            break;
        case 'j':
            snare.play();
            iconss[6].style.boxShadow = "0px 0px 10px yellow";
            iconss[6].style.transform = "scale(1.1)";
            break;
        case 'k':
            tom.play();
            iconss[7].style.boxShadow = "0px 0px 10px yellow";
            iconss[7].style.transform = "scale(1.1)";
            break;
        case 'l':
            tink.play();
            iconss[8].style.boxShadow = "0px 0px 10px yellow";
            iconss[8].style.transform = "scale(1.1)";
            break;
        default:
            alert("Enter Correct Key!!!");
            break;
    }
})
document.addEventListener('keyup',function(e){
    var keyEvent1 = e.key;
    switch(keyEvent1)
    {
        case 'a':
            clap.play();
            iconss[0].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
            iconss[0].style.transform = "scale(1)";
            break;
        case 's':
            hithat.play();
            iconss[1].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
            iconss[1].style.transform = "scale(1)";
            break;
        case 'd':
            kick.play();
            iconss[2].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
            iconss[2].style.transform = "scale(1)";
            break;
        case 'f':
            openhat.play();
            iconss[3].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
            iconss[3].style.transform = "scale(1)";
            break;
        case 'g':
            boom.play();
            iconss[4].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
            iconss[4].style.transform = "scale(1)";
            break;
        case 'h':
            ride.play();
            iconss[5].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
            iconss[5].style.transform = "scale(1)";
            break;
        case 'j':
            snare.play();
            iconss[6].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
            iconss[6].style.transform = "scale(1)";
            break;
        case 'k':
            tom.play();
            iconss[7].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
            iconss[7].style.transform = "scale(1)";
            break;
        case 'l':
            tink.play();
            iconss[8].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
            iconss[8].style.transform = "scale(1)";
            break;
        default:
            alert("Enter Correct Key!!!");
            break;
    }
})
iconss[0].addEventListener('click',function(){
    clap.play();
    iconss[0].style.boxShadow = "0px 0px 10px yellow";
    iconss[0].style.transform = "scale(1.1)";
    setTimeout(function(){
    iconss[0].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
    iconss[0].style.transform = "scale(1)";
    },100);
})
iconss[1].addEventListener('click',function(){
    hithat.play();
    iconss[1].style.boxShadow = "0px 0px 10px yellow";
    iconss[1].style.transform = "scale(1.1)";
    setTimeout(function(){
    iconss[1].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
    iconss[1].style.transform = "scale(1)";
    },100);
})
iconss[2].addEventListener('click',function(){
    kick.play();
    iconss[2].style.boxShadow = "0px 0px 10px yellow";
    iconss[2].style.transform = "scale(1.1)";
    setTimeout(function(){
    iconss[2].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
    iconss[2].style.transform = "scale(1)";
    },100);
})
iconss[3].addEventListener('click',function(){
    openhat.play();
    iconss[3].style.boxShadow = "0px 0px 10px yellow";
    iconss[3].style.transform = "scale(1.1)";
    setTimeout(function(){
    iconss[3].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
    iconss[3].style.transform = "scale(1)";
    },100);
})
iconss[4].addEventListener('click',function(){
    boom.play();
    iconss[4].style.boxShadow = "0px 0px 10px yellow";
    iconss[4].style.transform = "scale(1.1)";
    setTimeout(function(){
    iconss[4].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
    iconss[4].style.transform = "scale(1)";
    },100);
})
iconss[5].addEventListener('click',function(){
    ride.play();
    iconss[5].style.boxShadow = "0px 0px 10px yellow";
    iconss[5].style.transform = "scale(1.1)";
    setTimeout(function(){
    iconss[5].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
    iconss[5].style.transform = "scale(1)";
    },100);
})
iconss[6].addEventListener('click',function(){
    snare.play();
    iconss[6].style.boxShadow = "0px 0px 10px yellow";
    iconss[6].style.transform = "scale(1.1)";
    setTimeout(function(){
    iconss[6].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
    iconss[6].style.transform = "scale(1)";
    },100);
})
iconss[7].addEventListener('click',function(){
    tom.play();
    iconss[7].style.boxShadow = "0px 0px 10px yellow";
    iconss[7].style.transform = "scale(1.1)";
    setTimeout(function(){
    iconss[7].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
    iconss[7].style.transform = "scale(1)";
    },100);
})
iconss[8].addEventListener('click',function(){
    tink.play();
    iconss[8].style.boxShadow = "0px 0px 10px yellow";
    iconss[8].style.transform = "scale(1.1)";
    setTimeout(function(){
    iconss[8].style.boxShadow = "0px 0px 10px rgb(90, 89, 89)";
    iconss[8].style.transform = "scale(1)";
    },100);
})