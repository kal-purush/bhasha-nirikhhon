function validateinput()  
{ 

var userName = document.myform.name.value;
var fname = document.myform.fatherName.value;
var messageInput = document.myform.message.value;
var x=document.myform.email.value;  
var atposition=x.indexOf("@");  
var dotposition=x.lastIndexOf(".");  


if (fname==null || fname=="" || userName==null || userName=="" || messageInput==null || messageInput=="" ){  
  alert(" please Enter something");  
  return false; 
} 
 if (atposition<1 || dotposition<atposition+2 || dotposition+2>=x.length){  
  alert("Please enter a valid e-mail address!!!");  
  return false;  
  }  

}