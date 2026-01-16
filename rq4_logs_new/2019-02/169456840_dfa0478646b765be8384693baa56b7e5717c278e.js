
/*EMAIL VALIDATION*/
function validateEmail(){
  var email = document.getElementById("email");
  var error = document.getElementById("errors");
  var emailReg = /^([\w-\.]+@([\w-]+\.)+[\w-]{2,6})?$/;
	if(!emailReg.test(email.value)){
  	error.innerHTML = "please enter a valid email address";
    error.style.color = "red";
    email.style.border = "3px solid red";
  }
  else if(emailReg.test(email.value)){
  	error.innerHTML = "valid email!";
    error.style.color = "green";
    email.style.border = "3px solid green";
  }
}

/*CALLBACK FUNCTION*/
function callBack(response) {
  var test = JSON.stringify(response.user.username);
  var ul = document.getElementById("users");
  var li = document.createElement("li");
  if(test != "undefined"){
    li.appendChild(document.createTextNode(test.split('"').join("")));
    ul.appendChild(li);

  }
  else if(test == "undefined"){
    var errors= document.getElementById("errors");
    errors.innerHTML = "Oups, something went wrong!";
    errors.style.color="red";
  }


  /*CLEAR INPUT ON SUBMIT*/
  document.getElementById("name").value = "";
  document.getElementById("email").value = "";  
  document.getElementById("errors").innerHTML = "";
  var emailInput = document.getElementById("email")
  var info = document.getElementById("info");
  emailInput.style.border = "1px solid grey";
  info.style.display = "block";


  //DELETE LIST ITEM
  li.onclick = function() {
    this.parentNode.removeChild(this);
    }
}



/*FUNCTION CALLED ON BUTTON CLICK*/
 function work(){
  event.preventDefault();
 	var username = document.getElementById("name").value;
 	var email = document.getElementById("email").value;
  
  if(username && email != ""){
  	addUser(username, email, callBack);
  }
  else if(username || email == ""){
  	var error = document.getElementById("errors")
    error.innerHTML = "Please fill both fields"
    error.style.color = "red";
  }
  
 }
 

function addUser(username, email, callback) {
  var response,
    success = (!!Math.round(Math.random()));
  if (!success) {
    response = {
      success: success, 
      error: "Oups, something went wrong!"
    };
  } else {
    response = {
      success: success,
      user: { 
        username: username,
        email: email
      }
    };
  }

  $.ajax({
    url: '/echo/json/',
    type: "post",
    data: { json: JSON.stringify(response)},
    success:  callback
  });
}

