const minutesDisplay=document.querySelector('.minute');
const secondsDisplay=document.querySelector('.second');
let minutes=15;
let seconds=0;
minutesDisplay.innerHTML=minutes;
secondsDisplay.innerHTML='0'+seconds;
let counter=setInterval(()=>{
if(seconds=== 0 && minutes!== 0)
{
  seconds=60;
   minutes--;

}
if(seconds>0)
{
  seconds--;
}
if(seconds>=10)
{
  secondsDisplay.innerHTML=seconds;
}
else {
  secondsDisplay.innerHTML='0'+seconds;
}
if(minutes>=10)
{
  minutesDisplay.innerHTML=minutes;
}
else {
  minutesDisplay.innerHTML='0'+ minutes;
}



},1000);