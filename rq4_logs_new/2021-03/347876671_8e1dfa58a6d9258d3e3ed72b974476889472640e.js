/* Get the values from the form */

window.onload = function (){
    var getAccount = document.getElementById('info-submit');
    getAccount.onclick = emailDetails()
};

function emailDetails(){

    var userDetails = [];

    userDetails.push(document.getElementById('name').Value);
    userDetails.push(document.getElementById('email').Value);
    userDetails.push(document.getElementById('pass').Value);

    userName = userDetails[0];
    userEmail = userDetails[1];
    userPass = userDetails[2];


    if (userPass.length < 8) {

        document.getElementById('alert-3').innerHTML = "The password you entered is too short"
        
    } else {
        document.getElementById('alert-3').innerHTML = "The password is Strong"
    }

}