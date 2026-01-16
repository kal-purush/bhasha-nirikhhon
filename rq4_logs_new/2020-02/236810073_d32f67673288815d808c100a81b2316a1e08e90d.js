const signUpButton=document.getElementById("signUp");
const signInButton=document.getElementById("signIn");
const container=document.getElementById("container");
signUpButton.addEventListener('click',() => container.classList.add('right-panel-active'));
signInButton.addEventListener('click',() => container.classList.remove('right-panel-active'));
function play(){
  var email=document.getElementById('email2');
  var password=document.getElementById('password');
  if(email.value==""){alert("Email can't be empty");return false;}
else if(password.value==""){alert("Password can't be empty");return false;}
}

function check(){
  var name=document.getElementById('name')
  var email=document.getElementById('email')
  var mobile=document.getElementById('mobile')
  var age=document.getElementById('age')
  var city=document.getElementById('city')
  var country=document.getElementById('country')
  var pswd=document.getElementById('pswd')
  var repswd=document.getElementById('repswd')
  if(name.value==""){alert("Name can't be empty!!");return false;}
  else if(email.value==""){alert("Email can't be empty!!");return false;}
  else if(mobile.value==""){alert("Mobile Number can't be empty!!");return false;}
  else if(age.value==""){alert("Age can't be empty!!");return false;}
  else if(city.value==""){alert("City name can't be empty!!");return false;}
  else if(country.value==""){alert("Country name can't be empty!!");return false;}
  else if(pswd.value==""){alert("Password can't be empty!!");return false;}
  else if(repswd.value==""){alert("Password can't be empty!!");return false;}

}