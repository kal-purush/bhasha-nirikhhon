let urlBase = 'http://www.cop4331-group2.com/api';
let extension = 'php';

let userId = 0;
let firstName = "";
let lastName = "";

function doSignUp()
{
	userId = 0;
	// firstName = "";
	// lastName = "";
	
	// Getting data from the html IDs
	let signupEmail = document.getElementById("signupEmail").value;
	let password = document.getElementById("signupPassword").value;
	//let hash = md5(password); // Hashing password
	
    document.getElementById("signupResult").innerHTML = "";
    
    if (signupEmail == "" && signupPassword == "") {
        document.getElementById("signupResult").innerHTML = "Please enter an email/password";
        return;
    }

    if (signupEmail == "") {
        document.getElementById("signupResult").innerHTML = "Please enter an email";
        return;
    }

    if (signupPassword == "") {
        document.getElementById("signupResult").innerHTML = "Please enter an password";
        return;
    }

	// Creating the jsonPayload
	let jsonPayload = '{"email" : "' + signupEmail + '", "pass" : "' + password + '"}';
	let url = urlBase + '/newuser.' + extension;

	let xhr = new XMLHttpRequest();
	xhr.open("POST", url, true);
	xhr.setRequestHeader("Content-type", "application/json; charset=UTF-8");
	try
	{
		// Sending json
		xhr.send(jsonPayload);

		xhr.onreadystatechange = function() 
		{
			if (this.readyState == 4 && this.status == 200) 
			{
		
				//firstName = jsonObject.firstName;
				//lastName = jsonObject.lastName;
    
                // Could make this go to home if the login happens during the signup
				window.location.href = "index.html";
			}
		};
		
	}
	catch(err)
	{
		document.getElementById("signupResult").innerHTML = err.message;
	}

}