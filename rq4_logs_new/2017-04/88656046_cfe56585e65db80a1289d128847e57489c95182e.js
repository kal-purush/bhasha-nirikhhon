function validate(userForm) {
    div=document.getElementById("emailmsg");
    div.style.color="red";
    if(div.hasChildNodes()) {
        div.removeChild(div.firstChild);
    }
    regex=/(^\w+\@\w+\.\w+)/;
    match=regex.exec(userForm.emailaddress.value);
    if(!match) {
        div.appendChild(document.createTextNode("Invalid Email!"));
        userForm.emailaddress.focus();
        return false;
    }

    div=document.getElementById("passwdmsg");
    div.style.color="red";
    if(div.hasChildNodes()) {
        div.removeChild(div.firstChild);
    }
    if(userForm.password.value.length <= 5) {
        div.appendChild(document.createTextNode("The password should be at least 6 characters long!"));
        userForm.password.focus();
        return false;
    }

    div=document.getElementById("repasswdmsg");
    div.style.color="red";
    if(div.hasChildNodes()) {
        div.removeChild(div.firstChild);
    }
    if(userForm.password.value != userForm.repassword.value) {
        div.appendChild(document.createTextNode("Passwords do not match!"));
        userForm.password.focus();
        return false;
    }

    var div = document.getElementById("fname");
    div.style.color="red";
    if(div.hasChildNodes()) {
        div.removeChild(div.firstChild);
    }
    if(userForm.firstName.value == 0) {
        div.appendChild(document.createTextNode("First Name cannot be blank!"));
        userForm.firstName.focus();
        return false;
    }return true;

    var div = document.getElementById("lname");
    div.style.color="red";
    if(div.hasChildNodes()) {
        div.removeChild(div.firstChild);
    }
    if(userForm.lastName.value == 0) {
        div.appendChild(document.createTextNode("Last Name cannot be blank!"));
        userForm.lastName.focus();
        return false;
    }return true;
}