var countryCode = document.querySelector("#countryCode");
var userNoInput = document.querySelector("#userNoInput");
var errorPara = document.querySelector(".errorPara");
var formatedNoDiv = document.querySelector(".formatedNoDiv");
var result = document.querySelector("#result");

function formatNo() {
    if (countryCode.value === "+92") {
        if (userNoInput.value.length != 10) {
            errorPara.classList.add("displayblock");
            errorPara.innerHTML = `You have entered ${userNoInput.value.length} numbers instead of "10" numbers.`;
            setTimeout(() => {
                errorPara.classList.remove("displayblock");
            }, 3000);
        } else {
            var countryName  = "Pakistan";
            var pkCode = "+92-";
            var dashed = userNoInput.value.slice(0, 3) + "-" + userNoInput.value.slice(3);
            var formatedNo = pkCode + dashed;
            formatedNoDiv.classList.add("displayblock");
            result.innerHTML = `<b>Country Name:</b> ${countryName}<br> <b>Formated Number:</b> `+ formatedNo;
        }
    } else if (countryCode.value === "+971") {
        if (userNoInput.value.length != 8) {
            errorPara.classList.add("displayblock");
            errorPara.innerHTML = `You have entered ${userNoInput.value.length} numbers instead of "8" numbers.`;
            setTimeout(() => {
                errorPara.classList.remove("displayblock");
            }, 3000);
        } else {
            var countryName = "United Arab Emmirates";
            var uaeCode = "+971-";
            var fomrateNO = userNoInput.value.slice(0, 1) + "-" + userNoInput.value.slice(1, 4) + "-" + userNoInput.value.slice(4);
            var formatedNo = uaeCode + fomrateNO;
            formatedNoDiv.classList.add("displayblock");
            result.innerHTML = `<b>Country Name:</b> ${countryName}<br> <b>Formated Number:</b> `+ formatedNo;
        }
    } else {
        errorPara.classList.add("displayblock");
        errorPara.innerHTML = `You have entered ${countryCode.value} instead of "+92" or "+971."`;
        setTimeout(() => {
            errorPara.classList.remove("displayblock");
        }, 3000);
    }
}

function checkCountryCode() {
    if(countryCode.value === '+92'){
        // userNoInput.setAttribute
        userNoInput.setAttribute("maxlength", "10");
    } else if (countryCode.value === "+971") {
        userNoInput.setAttribute("maxlength", "8");
    } else {
        userNoInput.value = "";
        errorPara.classList.add("displayblock");
        errorPara.innerHTML = `Enter correct country code.`;
        setTimeout(() => {
            errorPara.classList.remove("displayblock");
        }, 3000);
    }
}