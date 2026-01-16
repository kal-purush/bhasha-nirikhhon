function formValidation()
{
	
	var umobile = document.registration.phone;
	var uemail = document.registration.email;

	
		
    		if(validateMobile(umobile))
    		{
  				if(validateEmail(uemail))
				{
					
						}
		
	}
	return false;
}
 




function validateMobile(umobile)
{
	var mobno = /^\d{10}$/;
	if(umobile.value.match(mobno))
	{
		return true;
	}
	else
	{
		alert('Mobile Number must contain numbers only(exactly 10 digits)');
		umobile.focus();
		return false;
	}
}

function validateEmail(uemail)
{
	var mailformat = /^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/;
	if(uemail.value.match(mailformat))
	{
		return true;
	}
	else
	{
		alert("You have entered an invalid email address!");
		uemail.focus();
		return false;
	}
}


