//Question1
function Add(){
    var n1=document.getElementById("n1").value;
    var n2=document.getElementById("n2").value;
    result=parseInt(n1)+parseInt(n2);
    console.log(result);
    var x= document.getElementById("result").value=result;
  
    
}
function Minus(){
    var n1=document.getElementById("n1").value;
    var n2=document.getElementById("n2").value;
    result=parseInt(n1)-parseInt(n2);
    console.log(result);
    document.getElementById("result").value=result;
}
function Multi(){
    var n1=document.getElementById("n1").value;
    var n2=document.getElementById("n2").value;
    result=parseInt(n1)*parseInt(n2);
    console.log(result);
    document.getElementById("result").value=result;
}
function Div(){
    var n1=document.getElementById("n1").value;
    var n2=document.getElementById("n2").value;
    result=parseInt(n1)/parseInt(n2);
    console.log(result);
    document.getElementById("result").value=result;
}
function Mod(){
    var n1=document.getElementById("n1").value;
    var n2=document.getElementById("n2").value;
    result=parseInt(n1)%parseInt(n2);
    console.log(result);
    document.getElementById("result").value=result;
}
//Question2
function tinhToan(){
    var n1=document.getElementById("num1").value;
    var n2=document.getElementById("num2").value;
    chuvi=(parseInt(n1)+parseInt(n2))/2;
    dientich=parseInt(n1)*parseInt(n2);
    document.getElementById("chuVi").value=chuvi;
    document.getElementById("dienTich").value=dientich;
}
//Question3
function giaiPT(){
    var a=parseFloat(document.getElementById("a").value);
    var b=parseFloat(document.getElementById("b").value);
    var c=parseFloat(document.getElementById("c").value);
    const delta=parseFloat((b/2) * (b/2) - a * c);
    if (delta < 0)
    {
        document.getElementById("ketqua").innerHTML="Phuong trinh vo nghiem!!";
    }
    else if (delta == 0)
    {
        let x = parseFloat((-b/2)/a);
        document.getElementById("ketqua").innerHTML="PT co nghiem kep: x1 = x2="+ x;
        console.log(x);
    }
    else
    {
        var x1 = parseFloat(((-b / 2) - Math.sqrt(delta)) / a);
        var x2 = parseFloat(((-b / 2) + Math.sqrt(delta)) / a);
        document.getElementById("ketqua").innerHTML="PT co 2 nghiem pb:"+Math.round(x1 * 100) / 100+" va"+Math.round(x2 * 100) / 100;
      
    }

   
}
//Question 4
function caculate(){
    var n1=document.getElementById("number1").value;
    var n2=document.getElementById("number2").value;
    tong=parseInt(n1)+parseInt(n2);
    hieu=parseInt(n1)-parseInt(n2);
    tich=parseInt(n1)*parseInt(n2);
    chia=parseInt(n1)/parseInt(n2);
    if(document.getElementById("tong").selected==true){
        document.getElementById("q4-Result").value=tong;
    }
    if(document.getElementById("hieu").selected==true){
        document.getElementById("q4-Result").value=hieu;
    }
    if(document.getElementById("tich").selected==true){
        document.getElementById("q4-Result").value=tich;
    }
    if(document.getElementById("chia").selected==true){
        document.getElementById("q4-Result").value=Math.round(chia * 100) / 100;
    }
}