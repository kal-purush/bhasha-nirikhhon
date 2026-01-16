function parkTudorWebsite() {
    location.href = 'https://www.parktudor.org';
}
function schoolDropDown() {
    var classToggle = document.getElementById("schoolToggle").className;
    if (classToggle === "fa fa-plus") {
        document.getElementById("schoolToggle").className = "fa fa-minus";
        document.getElementById("myDropdown").classList.toggle("show");

        
    }
    else if (classToggle === "fa fa-minus") {
        document.getElementById("schoolToggle").className = "fa fa-plus";
        document.getElementById("myDropdown").classList.toggle("show");

    }
}
function majorDropDown() {
    var classToggle = document.getElementById("majorToggle").className;
    if (classToggle === "fa fa-plus") {
        document.getElementById("majorToggle").className = "fa fa-minus";
        document.getElementById("myDropdownA").classList.toggle("show");
    }
    else if (classToggle === "fa fa-minus") {
        document.getElementById("majorToggle").className = "fa fa-plus";
        document.getElementById("myDropdownA").classList.toggle("show");
    }
}
function stateDropDown() {
    var classToggle = document.getElementById("stateToggle").className;
    if (classToggle === "fa fa-plus") {
        document.getElementById("stateToggle").className = "fa fa-minus";
        document.getElementById("myDropdownB").classList.toggle("show");
    }
    else if (classToggle === "fa fa-minus") {
        document.getElementById("stateToggle").className = "fa fa-plus";
        document.getElementById("myDropdownB").classList.toggle("show");
    }
}