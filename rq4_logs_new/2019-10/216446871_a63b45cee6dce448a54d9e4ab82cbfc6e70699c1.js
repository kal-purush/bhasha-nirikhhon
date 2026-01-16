// DATE OBJECT
/*
let date1=new Date(); // this date object for creating date in your application

var year=date1.getFullYear();
var month=date1.getMonth();
var hours=date1.getHours();
var minutes=date1.getMinutes();
var seconds=date1.getSeconds();
var milisecs=date1.getMilliseconds();
var time=date1.getTime();
var now=Date.now();

console.log(`${year}`);
console.log(`${month}`);
console.log(`${hours}:${minutes}:${seconds}`);
console.log(`${time}`);
console.log(`${now}`);
*/

// CREATE A CLOCK HERE
/*
var date2=new Date();
var h=date2.getHours();
var m=date2.getMinutes();
var s=date2.getSeconds();

console.log(`${h}:${m}:${s}`);


// TIME DELAY

var username3='Sweety';
var username4='Souravi';

setTimeout(function()
{
    document.writeln(username3);
} , 1000);

setInterval(() => {
    document.writeln(username4);
}, 1000);
*/


// setInterval(() =>
// {
//     var date3=new Date();

//     var h1=date3.getHours();
//     var m1=date3.getMinutes();
//     var s1=date3.getSeconds();
//     var format=date3.getHours() < 12 ? 'am' : 'pm';

//     document.getElementById('clock').innerHTML=`${h1}:${m1}:${s1} ${format}`;
// } , 1000);


setInterval(() =>
{
   var date4=new Date();
   var time=date4.toLocaleTimeString();
   document.getElementById('clock').innerHTML=time; 
} , 1000);