var top=100;
var x=100;
var y=200;
var z=300;
var q=400;

var r = Math.floor(Math.random() * 255) + 1;
var g = Math.floor(Math.random() * 255) + 1;
var b = Math.floor(Math.random() * 255) + 1;

window.setInterval(function myFunction(){
   $("#one")
       .after('<div style="color: rgb('+r+','+g+','+b+'); margin-left: '+x+'px; margin-top: '+top+'px">ily</div>');
   $("#two")
       .after('<div style="color: rgb('+g+','+b+','+r+'); margin-left: '+y+'px; margin-top: '+top+'px">happy</div>');
   $("#three")
       .after('<div style="color: rgb('+b+','+r+','+g+'); margin-left: '+z+'px; margin-top: '+top+'px">birthday</div>');
   $("#four")
       .after('<div style="color: rgb('+r+','+b+','+g+'); margin-left: '+q+'px; margin-top: '+top+'px">madi</div>');
  
  x+=5;
  y-=10;
  z-=15;
  q += 10;
  
  if(top > -200){
     top -= 10;
  } else {
     top = Math.floor(Math.random() * 500) + 1;
     x = Math.floor(Math.random() * 500) + 1;
     y = Math.floor(Math.random() * 500) + 1;
     z = Math.floor(Math.random() * 500) + 1;
     q = Math.floor(Math.random() * 500) + 1;
  }
  
  r = Math.floor(Math.random() * 255) + 1;
  g = Math.floor(Math.random() * 255) + 1;
  b = Math.floor(Math.random() * 255) + 1;

}, 1000);