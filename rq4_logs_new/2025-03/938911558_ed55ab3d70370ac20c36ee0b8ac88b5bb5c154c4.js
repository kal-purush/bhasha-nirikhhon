let userData = {
    name: "",
    email: "",
    status: "Active",
    gender: "Male"
};
let formEl = document.getElementById("myform");
let nameInputEl = document.getElementById("name")
let nameErrorMsgEl = document.getElementById("nameErrorMsg")
let emailInputEl = document.getElementById("email")
let emailErrMsgEl = document.getElementById("emailErrMsg")
let workingStatusEl = document.getElementById("status")
let genderMaleEl = document.getElementById("genderMale")
let genderFemaleEl = document.getElementById("genderFemale")

function submitFormData(data) {
    let url = "https://gorest.co.in/public-api/users";
    let options = {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
            Authorization: "Bearer c559ec7a1d89c79e2322ff6c3a7ccc4732da4a0600c767d9de0e97aeca4debe5"
        },
        body: JSON.stringify(userData)
    };
    fetch(url, options)
        .then(function(response) {
            console.log(response.status);
            return response.json();
        })
        .then(function(jsonData) {
            console.log(jsonData);
            if (jsonData.code === 422) {
                let errMsg = jsonData.data[0].field + " " + jsonData.data[0].message;
                console.log(errMsg);
                emailErrMsgEl.textContent = errMsg
            }
        })
}

genderFemaleEl.addEventListener("change", function() {
    userData.gender = genderFemaleEl.value;
    console.log(userData)

})
genderMaleEl.addEventListener("change", function() {
    userData.gender = genderMaleEl.value;
    console.log(userData)
})
workingStatusEl.addEventListener("change", function(event) {
    userData.status = event.target.value;
    console.log(userData);

})
nameInputEl.addEventListener("blur", function() {
    if (nameInputEl.value === "") {
        nameErrorMsgEl.classList.remove("hidden")
    } else {
        nameErrorMsgEl.classList.add("hidden");
    }
    userData.name = event.target.value;
})
emailInputEl.addEventListener("blur", function(event) {
    if (event.target.value === "") {
        emailErrMsgEl.textContent = "Required *"
    } else {
        emailErrMsgEl.textContent = ""
    }
    userData.email = event.target.value;
})
formEl.addEventListener("submit", function(event) {
    event.preventDefault();
    submitFormData(userData);
})