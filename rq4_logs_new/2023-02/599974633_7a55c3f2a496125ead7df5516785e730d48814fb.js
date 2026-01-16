document.getElementById("button1").addEventListener("click",function hello(){
		document.querySelector(".popup").style.display="flex";
		}
)
document.querySelector(".close").addEventListener("click",function hello(){
		document.querySelector(".popup").style.display="none";
}
)
function buy(service){
	alert("Thank you for choosing our "+service+" service!!");	
}
function check(){
	var mailformat =/^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$/ ;
	var inputText=(document.getElementById("lblmail").value);
	if (document.getElementById('lbluser').value==""){
	alert('User-name not found in the our library');
	}
	else if (!inputText.match(mailformat)){
		alert('Email-ID not found in the our library.');
	}
	else if (document.getElementById('lbltext').value==""){
		alert('Sorry!! \nWe do not accept blank query.');
	}
	else{
		alert("Thank you for reaching out to us!! \n We will process your query as soon as possible.")
	}
}
	

function checkforblank(){   //need to change mailformat conditions
	var mailformat =/^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$/ ;
	var inputText=(document.getElementById("mail").value);
	if (document.getElementById('user').value==""){
	alert('Please enter a valid User-name');
	}
	else if (!inputText.match(mailformat)){
		alert('Please enter a valid Email-ID: \n eg: abc@xyz.com');
	}
	else if (document.getElementById('pass').value==""){
		alert('Please enter a valid Password');
	}
	else{
		alert("Signed in succesfully");
		document.querySelector(".popup").style.display="none";
	}
}