function wage(){

let hour = document.getElementById("hour").value;
let price = document.getElementById("price").value;

var today = new Date();
var month = today.getMonth();
let monthly = hour*price*4;
let intrest = (monthly*2.5)/(100);
let a =  "Number of hours : "+hour+"Amount for hour: "+price+"Monthly amount: "+monthly+"Intrest rate 2.5%: "+intrest+"dollors";
document.getElementById("abc").innerHTML = "Number of hours : "+hour+"<br>Amount for hour: "+price+"<br>Monthly amount: "+monthly+"<br>Intrest rate 2.5%: "+intrest;
alert("Number of hours : "+hour+"\nAmount for hour: "+price+"\nMonthly amount: "+monthly+"\nIntrest rate 2.5%: "+intrest);
responsiveVoice.speak(a);

}
//reference from responsivevoice.js api