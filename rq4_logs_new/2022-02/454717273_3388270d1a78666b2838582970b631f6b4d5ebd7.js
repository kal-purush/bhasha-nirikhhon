function append(){
    let code=document.getElementById("target").innerHTML;
    let now=document.getElementById("first").value;
    document.getElementById("target").innerHTML=code+"<br>"+now;
}