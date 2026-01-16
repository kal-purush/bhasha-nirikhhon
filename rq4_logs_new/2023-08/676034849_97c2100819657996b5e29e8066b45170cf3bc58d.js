const hour = document.getElementById('hour');
const minute = document.getElementById('minute');
const second = document.getElementById('second');
 
const start = document.getElementById('start');
const reset = document.getElementById('reset');

const disp= document.getElementById('disp');

var interval = null;
var total = 0;

totalValue = () =>{
    total = Number(hour.value)*3600 + Number(minute.value)*60 + Number(second.value);
}

Timer = () =>{
    totalValue();
    total--;
    var audio = new Audio('digital-alarm-clock-151920.mp3');

    if(total >= 0)
    {
        var hr = Math.floor(total/3600);
        var mt = Math.floor((total/60) - (hr*60));
        var sc = total - ((hr*3600) + (mt*60));

        hour.value = hr;
        minute.value = mt;
        second.value = sc;
    }
    else
    {
        disp.innerText="Time Over!";
        audio.play(); 
    }
}

start.addEventListener('click',()=>{
    clearInterval(interval);
    interval = setInterval(Timer, 1000);

    disp.innerText = "Timer Started"; 
});

reset.addEventListener('click', ()=>{
    clearInterval(interval);
    hour.value=0;
    minute.value=0;
    second.value=0;
    disp.innerText = "Timer"; 
});
