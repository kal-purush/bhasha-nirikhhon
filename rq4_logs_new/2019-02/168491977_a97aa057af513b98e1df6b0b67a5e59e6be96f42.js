var a=function(){
    document.body.innerHTML="Hello a!";
}
var b =function(){
    document.body.innerHTML+="Hello b!";
}
a();
b();
var call = function(name){
    return "Hello"+name;
}
alert(call("Ritvik Jain"));