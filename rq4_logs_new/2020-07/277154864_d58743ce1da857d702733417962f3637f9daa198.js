

function myFunction1() {
  var x = Math.floor((Math.random() * 1000) + 1);
  document.getElementById("random1").value= x;
}
function myFunction2() {
  var x = Math.floor((Math.random() * 1000) + 1);
  document.getElementById("random2").value= x;
}
function myFun2() {
  var x = document.getElementById("random2").value;
  document.getElementById("rand2").innerHTML = scale;
}
function mapp()
{
    var numm=document.getElementById("random1").value;
    var t;
    t=(1+((numm-0)(3-0))/(1000-0));
    document.getElementById("rand1").innerHTML = t;
    return t;
    
}