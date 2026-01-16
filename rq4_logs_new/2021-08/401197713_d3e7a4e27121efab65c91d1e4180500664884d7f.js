// Topic 2 Javascript Functions

//Javascript Function
<!DOCTYPE html>
<html>
<body>

<h2>JavaScript Functions</h2>

<p>This example calls a function which performs a calculation and returns the result:</p>

<p id="demo"></p>

<script>
var x = myFunction(4, 3);
document.getElementById("demo").innerHTML = x;

function myFunction(a, b) {
  return a * b;
}
</script>

</body>
</html>

TODO: Change Activity

//Activity: Function
<html>
<head>
<title>Sample Page</title>
</head>
<body>
<h1>Javascript Function</h1>

<script>
	var r = 4;
	function area(r) {
		return Math.PI*r*r;
	} 
    document.write('Area = '+area(r));
</script>
</body>
</html>

//Function Expression
<!DOCTYPE html>
<html>
<body>

<p>After a function has been stored in a variable,
the variable can be used as a function:</p>

<p id="demo"></p>

<script>
var x = function (a, b) {return a * b};
document.getElementById("demo").innerHTML = x(4, 3);
</script>

</body>
</html>

//Arrow Function
<!DOCTYPE html>
<html>
<body>

<h2>JavaScript Arrow Function</h2>

<p>This example shows the syntax of an Arrow Function, and how to use it.</p>

<p id="demo"></p>
ß
<script>
var hello;

hello = () => {
  return "Hello World!";
}

document.getElementById("demo").innerHTML = hello();
</script>

</body>
</html>

//Arrow Function with Parameter
<!DOCTYPE html>
<html>
<body>

<h2>JavaScript Arrow Function</h2>

<p>This example shows that if you have only one parameter in an Arrow Function, you can skip the parentheses.</p>

<p id="demo"></p>

<script>
var hello;

hello = val => "Hello " + val;

document.getElementById("demo").innerHTML = hello("Universe!");
</script>

</body>
</html>

//Activity: Arrow Function
<html>
<head>
<title>Sample Page</title>
</head>
<body>
<h1>Javascript Function</h1>

<script>
	var r = 4;
	area = (r) =>  Math.PI*r*r;
    document.write('Area = '+area(r));
</script>
</body>
</html>

//Function Local Scope
<!DOCTYPE html>
<html>
<body>

<h2>JavaScript Scope</h2>

<p>Outside myFunction() carName is undefined.</p>

<p id="demo1"></p>

<p id="demo2"></p>

<script>
myFunction();

function myFunction() {
  var carName = "Volvo";
  document.getElementById("demo1").innerHTML = typeof carName + " " + carName;
}
document.getElementById("demo2").innerHTML = typeof carName;
</script>

</body>
</html>

//Global Scope
<!DOCTYPE html>
<html>
<body>

<h2>JavaScript Scope</h2>

<p>A GLOBAL variable can be accessed from any script or function.</p>

<p id="demo"></p>

<script>
var carName = "Volvo";
myFunction();

function myFunction() {
  document.getElementById("demo").innerHTML = "I can display " + carName;
}
</script>

</body>
</html>

//this keyword
<!DOCTYPE html>
<html>
<body>

<h2>The JavaScript <i>this</i> Keyword</h2>

<p>In this example, <b>this</b> represents the <b>person</b> object.</p>
<p>Because the person object "owns" the fullName method.</p>

<p id="demo"></p>

<script>
// Create an object:
var person = {
  firstName: "John",
  lastName : "Doe",
  id     : 5566,
  fullName : function() {
    return this.firstName + " " + this.lastName;
  }
};

// Display data from the object:
document.getElementById("demo").innerHTML = person.fullName();
</script>

</body>
</html>

//Block scope
<!DOCTYPE html>
<html>
<body>

<h2>Declaring a Variable Using let</h2>

<p id="demo"></p>

<script>
var  x = 10;
// Here x is 10
{  
  let x = 2;
  // Here x is 2
}
// Here x is 10
document.getElementById("demo").innerHTML = x;
</script>

</body>
</html>

//const
<!DOCTYPE html>
<html>
<body>

<h2>JavaScript const</h2>

<p>You can NOT reassign a constant array:</p>

<p id="demo"></p>

<script>
try {
  const cars = ["Saab", "Volvo", "BMW"];
  cars = ["Toyota", "Volvo", "Audi"];
}
catch (err) {
  document.getElementById("demo").innerHTML = err;
}
</script>

</body>
</html>

//Javascript Event

<html>
<head>
<title>Sample Page</title>
</head>
<body>
<h1>Find Element by ID</h1>

<button onclick="alert('Hello World')">Run</button>

</body>
</html>

//Ex: Javascript Event
<html xmlns="http://www.w3.org/1999/xhtml">

</head>
<body>

    <p><img src="https://i.ibb.co/h709QDx/merlion.jpg" id="merlion" alt="Merlion" width="350"  />.Click on the image </p>
<script>
	
	var image = document.getElementById("merlion");
	image.onclick = function(){
		alert("This is a Merlion");
	};
</script>
</body>
</html>