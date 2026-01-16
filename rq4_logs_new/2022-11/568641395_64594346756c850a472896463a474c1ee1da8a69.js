var twentyone = document.getElementById("twentyone");
var drake = document.getElementById("drake");

function jump() {
    if (twentyone.classList != "jump") { //only adds jump if twentyone classlist doesnt have jump. otherwise gives us false and this doesnt happen
        twentyone.classList.add("jump");

        setTimeout(function() {
            twentyone.classList.remove("jump");
        }, 300); //removes css animation 'jump' after 300ms so you can keep pressing key
    }
}

//detecting collision or alive
let isAlive = setInterval(function(){ //isAlive performs the function every 10 ms

    //gets position of your two things
    let twentyoneTop = parseInt(window.getComputedStyle(twentyone).getPropertyValue("top")); //gets the top y position of 21 as an integer
    let drakeLeft = parseInt(window.getComputedStyle(drake).getPropertyValue("left")); //gets the left x value of drake

    /*detect collision by seeing if drake is somewhere in 21 pixels (between 0-100px)
    detect if dino y position overlap with drake y? */
    if (drakeLeft < 100 && drakeLeft > 0 && twentyoneTop >= 280) {
        //collision
        alert("Drake got you! It's over!");
    }

}, 10);

document.addEventListener("keydown", function(event){ //jump on keydown
    jump();
});

document.addEventListener("click", function(event){ //jump on click
    jump();
});