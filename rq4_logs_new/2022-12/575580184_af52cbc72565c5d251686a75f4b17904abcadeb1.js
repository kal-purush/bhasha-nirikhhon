var regEmail = /^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/;
var regPhone = /^\d{10}$/;
var regName = /\d+$/g;

function validateForm1() {

    let fname = document.forms["form1"]["fname"].value;
    if (fname.trim() == "" || regName.test(fname)) {
        alert("Please enter your first and last name properly");
        return false
    }
    let lname = document.forms["form1"]["lname"].value;
    if (lname.trim() == "" || regName.test(lname)) {
        alert("Please enter your first and last name properly");
        return false
    }
    let dob = document.forms["form1"]["dob"].value;
    let email = document.forms["form1"]["email"].value;
    if (email == "" || !regEmail.test(email)) {
        alert("Please enter your email address properly");
        return false
    }
    let father_fname = document.forms["form1"]["father_fname"].value;
    if (father_fname.trim() == "" || regName.test(father_fname)) {
        alert("Please enter your father's first and last name properly");
        return false
    }
    let father_lname = document.forms["form1"]["father_lname"].value;
    if (father_lname.trim() == "" || regName.test(father_lname)) {
        alert("Please enter your father's first and last name properly");
        return false
    }
    let mother_fname = document.forms["form1"]["mother_fname"].value;
    if (mother_fname.trim() == "" || regName.test(mother_fname)) {
        alert("Please enter your mother's first and last name properly");
        return false
    }
    let mother_lname = document.forms["form1"]["mother_lname"].value;
    if (mother_lname.trim() == "" || regName.test(mother_lname)) {
        alert("Please enter your mother's first and last name properly");
        return false
    }
    let gender = document.forms["form1"]["gender"].value;
    if (gender.length == 0) {
        alert("Select your gender");
        return false
    }
    let nationality = document.forms["form1"]["nationality"].value;
    if (nationality.trim() == "" || regName.test(nationality)) {
        alert("Please enter your nationality properly");
        return false
    }
    let Str_addr = document.forms["form1"]["Str_addr"].value;
    if (Str_addr.trim() == "" || regName.test(Str_addr)) {
        alert("Please enter your Street Address properly");
        return false
    }
    let city = document.forms["form1"]["city"].value;
    if (city.trim() == "" || regName.test(city)) {
        alert("Please enter your city properly");
        return false
    }
    let country = document.forms["form1"]["country"].value;
    if (country.trim() == "" || regName.test(country)) {
        alert("Please enter your country properly");
        return false
    }
    let telephone = document.forms["form1"]["telephone"].value;
    if (telephone == "" || !regPhone.test(telephone)) {
        alert("Please enter your 10 digit telephone number properly");
        return false
    }
    let mobile = document.forms["form1"]["mobile"].value;
    if (mobile.trim() == "" || !regPhone.test(mobile)) {
        alert("Please enter your 10 digit mobile number properly");
        return false
    }
    var form1_details = {
        firstName: fname,
        lastName: lname,
        dob,
        email,
        fatherFirstName: father_fname,
        fatherLastName: father_lname,
        motherFirstName: mother_fname,
        motherLastName: mother_lname,
        gender,
        nationality,
        Street_Address: Str_addr,
        city,
        country,
        telephone,
        mobile
    }
    localStorage.setItem('personal_details', JSON.stringify(form1_details))
    return false
}
function validateForm2() {

    let inst_name = document.forms["form2"]["Hinstitute-name"].value;
    if (inst_name.trim() == "" || regName.test(inst_name)) {
        alert("Please enter your institute-name properly.");
        return false
    }
    let HBoard = document.forms["form2"]["HSC-Board"].value;
    if (HBoard.length == 0) {
        alert("Please select your HSC board.");
        return false
    }
    let Sinstitute_name = document.forms["form2"]["Sinstitute-name"].value;
    if (Sinstitute_name.trim() == "" || regName.test(Sinstitute_name)) {
        alert("Please enter your SSC institute-name properly.");
        return false

    }
    let SBoard = document.forms["form2"]["SSC-Board"].value;
    if (SBoard.length == 0) {
        alert("Please select your SSC board.");
        return false
    }
    let currently_pursuing = document.forms["form2"]["currently-pursuing"].value;
    if (currently_pursuing.trim() == "" || regName.test(currently_pursuing)) {
        alert("Please enter your currently pursuing course.");
        return false

    }
    let current_institute = document.forms["form2"]["current-institute"].value;
    if (current_institute.trim() == "" || regName.test(current_institute)) {
        alert("Please Enter Your Current Educational Institute Name.");
        return false

    }
    let current_backlogs = document.forms["form2"]["current-backlogs"].value;

    var form2_details = {
        inst_name,
        HBoard,
        SBoard,
        Sinstitute_name,
        currently_pursuing,
        current_institute,
        current_backlogs

    }

    localStorage.setItem("educational_details", JSON.stringify(form2_details));

    return false
}