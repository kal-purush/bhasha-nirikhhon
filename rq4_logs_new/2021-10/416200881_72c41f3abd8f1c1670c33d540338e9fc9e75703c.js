function getInputValue()
{
//Name
	var last = document.getElementById("LastName").value;
	var first= document.getElementById("FirstName").value;
	var mid= document.getElementById("MidName").value;
//Address
	var num=document.getElementById("NumStreet").value;
	var bar=document.getElementById("Barangay").value;
	var city =document.getElementById("City").value;
    var prov = document.getElementById("Province").value;
    var reg = document.getElementById("Region").value;
 // Social Media
 	var email=document.getElementById("Email").value;
 	var cont=document.getElementById("Contact").value;
 	var nat=document.getElementById("Nationality").value;
//birthday
	var month= document.getElementById("Month").value;
	var day=document.getElementById("Day").value;
	var year=document.getElementById("Year").value;
	var age=document.getElementById("Age").value;


//Name
		if (last==''){
			alert("Fill up thte Form!!");
		}
		else if (first==''){
			alert("Fill up thte Form!!");
		}
		else if (mid==''){
			alert("Fill up thte Form!!");
		}
//Address
		else if (num==''){
			alert("Fill up thte Form!!");
		}
		else if (bar==''){
			alert("Fill up thte Form!!");
		}
		 else if (city==''){
            alert("Fill up thte Form!!");
        }
        else if (prov==''){
            alert("Fill up thte Form!!");
        }
        else if (reg==''){
            alert("Fill up thte Form!!");
        }
        else if (email==''){
        	alert("Fill up the Form!!");
        }
        else if(cont==''){
        	alert("Fill up the Form!!");
        }
        else if(nat==''){
        	alert("Fill up the Form!!");
        }
//Birthday
		else if (month==''){
			alert("Fill up the Form!!");
		}
		else if (day==''){
			alert("Fill up the Form!!");
		}
		else if (year==''){
			alert("Fill up the Form!!");
		}
		else if (age==''){
			alert("Fill up the Form!!");
		}
//Success
		else {
			alert("Register Success!!")
			
		}
		
	
}
function getInputClear()
{
	document.getElementById("LastName").value="";
	document.getElementById("ExtName").value="";
	document.getElementById("FirstName").value="";
	document.getElementById("MidName").value="";
	document.getElementById("NumStreet").value="";
	document.getElementById("Barangay").value="";
	document.getElementById("District").value="";
	document.getElementById("City").value="";
	document.getElementById("Province").value="";
	document.getElementById("Region").value="";
	document.getElementById("Email").value="";
	document.getElementById("Contact").value="";
	document.getElementById("Nationality").value="";
	document.getElementById("Month").value="";
	document.getElementById("Day").value="";
	document.getElementById("Year").value="";
	document.getElementById("Age").value="";
	document.getElementById("Municipality").value="";
	document.getElementById("Place").value="";
	document.getElementById("Reg").value="";
	document.getElementById("Train").value="";

}