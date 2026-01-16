const days=document.querySelector("#days")
const hours=document.querySelector("#hour")
const minutes=document.querySelector("#minutes")
const second=document.querySelector("#second")

const currentYear=new Date().getFullYear();

const newYearTime=new Date(`january 1 ${currentYear+1} 00:00:00`)

function updateCountdowntime() {
    const currentTime=new Date()
    const diff=newYearTime-currentTime

    const d= Math.floor(diff/1000/60/60/24);
    const h= Math.floor(diff/1000/60/60) % 24;
    const m= Math.floor(diff/1000/60) % 60;
    const s= Math.floor(diff/1000) %60; 
    
   days.innerHTML=d;
   hours.innerHTML=h<10 ? "0" + h : h;
   minutes.innerHTML=m<10 ? "0" + m : m;
   second.innerHTML=s<10 ? "0" + s : s;

}
setInterval(updateCountdowntime,1000)