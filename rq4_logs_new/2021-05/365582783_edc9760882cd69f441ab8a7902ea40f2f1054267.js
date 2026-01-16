$('.message a').click(function(){$('form').animate({height: "toggle",opacity: "toggle"},"slow");});




const pass_field = document.querySelector("input");
const show_btn = document.querySelector("i");
show_btn.addEventListener("click", function(){

if(pass_field.type === "password"){
	pass_field.type = "text";
	show_btn.classList.add("hide");
}
else{
	pass_field.type = "password";
	show_btn.classList.remove("hide");

	}
});
myInput.onfocus = function() {
	document.getElementById("message").style.display = "block";
  }
  myInput.onblur = function() {
	document.getElementById("message").style.display = "none";
  }
  myInput.onkeyup = function() {
	var lowerCaseLetters = /[a-z]/g;
	if(myInput.value.match(lowerCaseLetters)) {  
	  letter.classList.remove("invalid");
	  letter.classList.add("valid");
	} else {
	  letter.classList.remove("valid");
	  letter.classList.add("invalid");
	}
	var upperCaseLetters = /[A-Z]/g;
	if(myInput.value.match(upperCaseLetters)) {  
	  capital.classList.remove("invalid");
	  capital.classList.add("valid");
	} else {
	  capital.classList.remove("valid");
	  capital.classList.add("invalid");
	}
	var numbers = /[0-9]/g;
	if(myInput.value.match(numbers)) {  
	  number.classList.remove("invalid");
	  number.classList.add("valid");
	} else {
	  number.classList.remove("valid");
	  number.classList.add("invalid");
	}
	if(myInput.value.length >= 6) {
	  length.classList.remove("invalid");
	  length.classList.add("valid");
	} else {
	  length.classList.remove("valid");
	  length.classList.add("invalid");
	}
  }
	
/*function isEmail(email) {
	return /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/.test(email);
}*/






