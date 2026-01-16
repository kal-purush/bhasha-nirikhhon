"use strict"
function literally_function() {
    var name = document.getElementById("input-name").value;
    var age = document.getElementById("input-age").value
    var gender;
    var gender_radios = document.getElementsByName("gender");
    for (var i = 0; i < gender_radios.length; ++i)
    if (gender_radios[i].checked == true)
    {
        gender = gender_radios[i].value;
    }
    // gender = document.querySelector('input[name="gender"]:checked').value;
    var city = document.getElementById("input-city").value;
    var dob = document.getElementById("input-dob").value;
    var department = document.querySelectorAll('input[type="checkbox"]:checked');
    if (!name || !age || !gender || !city || !dob || !department) 
    {
        alert("Please fill all info!");
        return;
    }
    var message = "Name: " + name + "\nAge: " + age 
    + "\nGender:" + gender + "\nCity: " + city;
    message += "\nDoB: " + dob.substr(8,2) + "/" + dob.substr(5,2) + "/" + dob.substr(0,4);
    message += "\nDepartment: " 
    for (var i = 0; i < department.length; ++i)
    {
        if (i == 0)  message += department[i].value;
        else message += ", " + department[i].value;
    } 
    if (department.length == 0) message += "none";
    alert(message);
  }

document.getElementById("submit").addEventListener("click",literally_function)