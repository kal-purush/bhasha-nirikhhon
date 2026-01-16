

var timer = 60;
var score = 0;
var hitrn = 0;

function makeBubble()
{
    var clutter = "";

    for (var i = 1; i < 191; i++)
    {
        var rn = Math.floor(Math.random() * 10);
        clutter += `<div class="bubble">${rn}</div>`;
    }

    document.querySelector("#pbtm").innerHTML = clutter;
}

function runTimer()
{
    var timerint = setInterval(function ()
    {
        if (timer > 0)
        {
            timer--;
            document.querySelector("#timerValue").textContent = timer;
        }
        else
        {
            clearInterval(timerint);
            document.querySelector("#pbtm").innerHTML = `<h1> Game Over </h1>`
        }
    }, 1000)
}

function getNewHit()
{
    hitrn = Math.floor(Math.random() * 10);
    document.querySelector("#hitValue").textContent = hitrn;
}

function increaseScore()
{
    score += 10;
    document.querySelector("#scoreValue").textContent = score;
}

document.querySelector("#pbtm")
    .addEventListener("click", function (dets)
    {
        var clickedNumber = Number(dets.target.textContent);
        if (clickedNumber === hitrn)
        {
            increaseScore();
            makeBubble();
            getNewHit();
        }
    })

makeBubble();
runTimer();
getNewHit();

