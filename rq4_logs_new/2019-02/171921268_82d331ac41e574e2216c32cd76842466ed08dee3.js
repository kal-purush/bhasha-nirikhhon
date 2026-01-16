//console.log("hello world!")
//alert("hello world!")

//1
var x = parseInt(prompt("Enter number", 0))
var y = parseInt(prompt("Enter number", 0))
if (x > y)
  {
    alert(x)
  }
else 
  {
   alert(y)
  }

//2
//
var x2 = parseInt(prompt("Enter number", 0))
var y2 = parseInt(prompt("Enter number", 0))
var z2 = parseInt(prompt("Enter number", 0))
if (x2 > y2 && x2 > z2)
  {
    alert(x)
  }
else if (y2 > z2)
  {
   alert(y2)
  }
else
  {
    alert(z2)
  }


//3
var a3 = parseInt(prompt("Enter number", 0))
var b3 = parseInt(prompt("Enter number", 0))

if (b3 == 0)
  {
    alert("cannot divide by zero")
  }
else
  {
    alert(a3/b3)
  }

//4
var a4 = parseInt(prompt("Enter positive number", 0))
if (a4 <10)
  {
    alert(1)
  }
else if (a4 < 100)
  {
    alert(2)
  }
else if(a4 < 1000)
  {
    alert(3)
  }
else 
  {
    alert("more than 3 digits")
  }

//5
var sum = 0
do
  {
    var a5 = parseInt(prompt("Enter numbers (0 to stop)", 0))
    sum = sum + a5
  }
while(a5 != 0)
alert(sum)

//6
var a6 = parseInt(prompt("Enter numbers (negative to stop)", 0))
var sum6 = 0
while(a6 >= 0)
  {
    sum6 = sum6 + a6;
    a6 = parseInt(prompt("Enter numbers (negative to stop)", 0))
  }
alert(sum6)
//7

var a7 = parseInt(prompt("Enter numbers (negative to stop)", 0))
var sum7 = 0
var i = 0
while(a7 >= 0)
  {
    sum7 = sum7 + a7;
    i++;
    a7 = parseInt(prompt("Enter numbers (negative to stop)", 0))
  }
if(i > 0)
  {
		alert(sum7 / i)
  }




