function show1(){
    if(document.getElementById('radio1').checked) 
    { 
        document.getElementById('inptVal').style.display ='block';    
} else { 
  alert ("You must select a button"); 
  return false; 
} 
}
function show2(){
    if(document.getElementById('radio2').checked) { 
        document.getElementById('uploadFl').style.display = 'block';
} else { 
  alert ("You must select a button"); 
  return false; 
} 
}

function Calculate(){
   const e=2.71828;    
   let a,b,c,d,p1, p2, p3, p4;
   let x=document.getElementById("at");
   let y=document.getElementById("ws");
   let z=document.getElementById("rh"); 

   let x_1=Number(x.value); // AIR TEMPERATURE
   let x_2=Number(y.value); // WIND SPEED
   let x_3=Number(z.value); // RELATIVE HUMIDITY


        if (x_1 && x_2) {
                a = 5.8;
                b = 0.02;
                c = -7.9;
                d = 0;
                p1 = 1 / (1 + Math.exp(-(a + b * x_1 + c * x_2)));
                p1.toFixed();
                document.getElementById("result1").innerHTML =p1;                        
       }else if (x_2 && x_3) {
                a = 4.5;
                b = 1.0;
                c = -6.4;
                d = -6.8;
                p2 = 1 / (1 + Math.exp(-(a + c * x_2 + d * x_3)));
                document.getElementById("result2").innerHTML =p2;                        

       }else if (x_1 && x_3) {
                a = 0.7;
                b = 2.2;
                c = -0.02;
                d = -6.8;
                p3 = 1 / (1 + Math.exp(-(a + b * x_1 + d * x_3)));
                document.getElementById("result3").innerHTML =p3;                        

       }else if (x_1 && x_2 && x_3) {
                a = 4.6;
                b = 0.96;
                c = 0.02;
                d = -6.8;
                p4 = 1 / (1 + Math.exp(-(a + b * x_1 + c * x_2 + d * x_3)));
                document.getElementById("result4").innerHTML =p4;                        

               
       }
   }
   