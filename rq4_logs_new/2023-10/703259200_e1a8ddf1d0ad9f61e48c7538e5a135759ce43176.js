// check for empty input boxes
// check for each criteria
function validateFunction(){

    // Get input from form elements
    const firstNameEl = document.querySelector("#fName").value;
    const firstNameErrorEl = document.querySelector("#fName_error_msg");

    const lastNameEl = document.querySelector("#lName").value;
    const lastNameErrorEl = document.querySelector("#lName_error_msg");

    // Check Names for Letters
    let nameRegex = /^[a-zA-Z]+$/;
    
    if (nameRegex.test(firstNameEl)) {

    } else {
        firstNameErrorEl.textContent = "* Please enter only letters";
    }
    if (nameRegex.test(lastNameEl)) {
        
    } else {
        lastNameErrorEl.textContent = "* Please enter only letters";
    }

}