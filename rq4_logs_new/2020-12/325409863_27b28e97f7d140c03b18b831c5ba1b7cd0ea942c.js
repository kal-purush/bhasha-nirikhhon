

async function getText() {
  var myObject = await fetch("http://localhost:8080/RestApiPractising/webapi/student/get/2");
  var myText = await myObject.text();
  document.getElementById("container").innerHTML = myText;
}

window.onload = getText