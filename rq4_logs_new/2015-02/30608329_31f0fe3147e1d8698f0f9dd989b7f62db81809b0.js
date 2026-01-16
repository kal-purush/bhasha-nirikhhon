var sumando1;
var nop=true;
function one(){
  document.getElementById("display").innerHTML = "1";
}

function zero(){	
  document.getElementById("display").innerHTML = "0";
}

function add(){	
  if(document.getElementById("display").innerHTML === ""){
    return;
  }
  sumando1 = document.getElementById("display").innerHTML;
  document.getElementById("display").innerHTML = "";
  nop=false;
}

function suma(){	
  var sumando2 = document.getElementById("display").innerHTML;
  if (sumando2 === "" || nop){
    return;
  }
  document.getElementById("display").innerHTML =(parseInt(sumando1) + parseInt(sumando2)) % 2;
  nop=false;
}

function limpia(){
  document.getElementById("display").innerHTML = "";
}













