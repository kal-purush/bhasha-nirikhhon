sum=0;
String="";
function Add(){
    a=parseInt(document.getElementById("data1").value);
    sum+=a;
    if(String.length==0){
        String+=sum;
    }
    else{
        String+=" + "+a;
    }
    document.getElementById("result").innerHTML = String;    
}
function sub(){
    a=parseInt(document.getElementById("data1").value);
    sum-=a;
    if(String.length==0){
        String+=sum;
    }
    else{
        String+=" - "+a;
    }
    document.getElementById("result").innerHTML = String;    
}
function mul(){
    a=parseInt(document.getElementById("data1").value);
    sum*=a;
    if(String.length==0){
        String+=sum;
    }
    else{
        String+=" * "+a;
    }
    document.getElementById("result").innerHTML = String;    
}
function div(){
    a=parseInt(document.getElementById("data1").value);
    sum/=a;
    if(String.length==0){
        String+=sum;
    }
    else{
        String+=" / "+a;
    }
    document.getElementById("result").innerHTML = String;    
}
function equal(){
    document.getElementById("result").innerHTML =String+" = "+ sum;
}