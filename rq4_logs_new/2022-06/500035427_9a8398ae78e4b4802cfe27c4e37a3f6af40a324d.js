let screen = document.querySelector('#screen');
let btn = document.querySelectorAll('.btn');
let btns = document.querySelectorAll('.btns');

for (item of btn) 
{
    item.addEventListener('click', (event) => 
    {
        btntext = event.target.innerText;

        if (btntext == '×') btntext = '*';
        if (btntext == '÷') btntext = '/';
        screen.value = screen.value + btntext;
    });
}

function sin() 
{
    screen.value = Math.sin(screen.value);
}

function cos() 
{
    screen.value = Math.cos(screen.value);
}

function tan() 
{
    screen.value = Math.tan(screen.value);
}

function pow() 
{
    screen.value = Math.pow(screen.value, 2);
}

function sqrt() 
{
    screen.value = Math.sqrt(screen.value, 2);
}

function log() 
{
    screen.value = Math.log10(screen.value);
}

function ln()
{
    screen.value = Math.log(screen.value);
}

function pi() 
{
    screen.value = 3.14159265359;
}

function e() 
{
    screen.value = 2.71828182846;
}

function factorial() 
{
    let i, num, f = 1;
    num = screen.value;
    for (i = 1; i <= num; i++)  f = f*i;
    i = i-1;
    screen.value = f;
}

function backspace() 
{
    screen.value = screen.value.substr(0, screen.value.length - 1);
}

function x(i)
{
    screen.value = Math.pow(screen.value,i);
}

function inverse()
{
    screen.value = 1/(screen.value);
}

function radian()
{
    screen.value = (screen.value * Math.PI)/180;
}

function degree()
{
    screen.value = (screen.value * 180)/(Math.PI);
}

const colorSwitch = document.getElementById('input-color-switch');
colorSwitch.addEventListener('click',checkMode);

function checkMode()
{
    if(colorSwitch.checked)
    {
        darkModeOn();
    }
    else
    {
        darkModeOff();
    }
}
let display = document.getElementById("screen");
let container = document.getElementByClass("calculator");

function darkModeOn()
{
    document.body.classList.add("dark-mode");
    display.style.background = "linear-gradient(#2b2b2b, #275aff)";
    display.style.color = "white";
    for (item of btn) 
    {
        item.style.background = "linear-gradient(#2b2b2b, #275aff)";
        item.style.color = "white";
    }
    for(item of btns)
    {
        item.style.background = "linear-gradient(#2b2b2b, #275aff)";
        item.style.color = "white";
    }
    container.style.background = "linear-gradient(to right bottom,rgba(180, 178, 178, 0.7),rgba(92, 206, 238, 0.3))";
}
function darkModeOff()
{
    document.body.classList.remove("dark-mode");
    display.style.background = "linear-gradient(#6486f6, #f1a4f4)";
    display.style.color = "black";
    for (item of btn) 
    {
        item.style.background = "linear-gradient(#6486f6, #f1a4f4)";
        item.style.color = "black";
    }
    for(item of btns)
    {
        item.style.background = "linear-gradient(#6486f6, #f1a4f4)";
        item.style.color = "black";
    }
    container.style.background = "linear-gradient(to right bottom,rgba(255, 255, 255, 0.7), rgba(255, 255, 255, 0.3))";
}