var mail = document.getElementById("uname");
var form1 = document.getElementById("form1");

document.addEventListener("keydown", function (event) {
    // If the Enter key is pressed and the form field is not a textarea
    if (event.key === "Enter") {
        // Prevent the default form submission behavior
        event.preventDefault();
    }
});

var mailformat = /^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/;
function Validate() {
    var mailformat = /^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/;
    if (mail.value != "") {
        // mail
        if (mail.value.match(mailformat)) {
            return true;
        }
        else {
            document.getElementById("email-alert").innerHTML = "<b>Enter Valid Email id</b>"
            document.getElementById("email-alert").style.display = "block";
            return false;
        }
    } else {
        document.getElementById("email-alert").innerHTML = "<b>Email should not be empty</b>"
        document.getElementById("email-alert").style.display = "block";
        return false;
    }
}


mail.addEventListener('input', verify_email);



function verify_email() {

    if (mail.value.match(mailformat)) {
        document.getElementById("email-alert").style.display = "none";
    }
    else if (mail.value == "") {
        document.getElementById("email-alert").innerHTML = "<b>Email id should not be empty</b>"
        document.getElementById("email-alert").style.display = "block";
    }
    else {
        document.getElementById("email-alert").innerHTML = "<b>Enter valid Email id</b>"
        document.getElementById("email-alert").style.display = "block";
    }
}