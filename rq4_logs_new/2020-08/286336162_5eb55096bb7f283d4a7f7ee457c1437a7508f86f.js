
function validateformReg()
{  
var name=document.reg.name.value;  
var password=document.reg.password.value;  
var password1=document.reg.password1.value;  
var email=document.reg.email.value;
var valid=true;

if(valid)
{
       
       if (name==null || name=="")
       {  
         alert("Name can't be blank");  
         return false; 
       }
       else if(email==null || email=="")
       {
         alert("Email can't be blank");
         return false;  

       }
       else if( password==null || password=="")
       {
         alert("Password can't be blank"); 
         return false; 
       }
       else if(password.length<6)
       {  
         alert("Password must be at least 6 characters long.");
         return false;
       }
        else if(password!=password1)
       {
         alert("Password didnt match"); 
        return false; 
       }
                    

alert("Register Successfully"); 
}
} 

function validateformLog()
{   
var password=document.log.password.value;   
var email=document.log.email.value;
var password1="paysw2020";
var email1="payxcode@gmail.com";
var valid=true;

if(valid)
{
       
       if(email==null || email=="")
       {
         alert("Email can't be blank");
         return false;  
       }
       else if(email!=email1)
       {
         alert("Email is wrong");
         return false;  

       }
       else if( password==null || password=="")
       {
         alert("Password can't be blank"); 
         return false; 
       }
        else if(password!=password1)
       {
         alert("Password is Wrong"); 
        return false; 
       }
                    

alert("Login Successfully"); 
}
} 

  