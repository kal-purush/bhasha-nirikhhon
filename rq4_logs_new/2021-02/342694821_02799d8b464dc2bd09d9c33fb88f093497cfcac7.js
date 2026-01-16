const alphabetRegExp = /^[a-zA-Z]+$/;

const  emailRegExp = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;

const alphabetChecker =  (input) =>  alphabetRegExp.test(input);

const emailChecker =  (input) =>  emailRegExp.test(input);


const validateAlphabet = (input_id, error_name) =>  {
    let input_value = document.getElementById(input_id).value;
    if(input_value.length > 3){
    if(!alphabetChecker(input_value)){
    document.getElementById(error_name).textContent="Please only alphabets are allowed!";
    }else{
        document.getElementById(error_name).textContent="";
        isValid();
    }
    }
}



const validateEmail = (input_id, error_name) => {
    let input_value = document.getElementById(input_id).value;
    if(input_value.length > 3){
    if(!emailChecker(input_value)){
        document.getElementById(error_name).textContent="Please provide a valid email address";
    }else{
        document.getElementById(error_name).textContent="";  
        isValid();
    }    
}
}

const chkPasswordLength = (input_id, error_name) => {
    let input_value = document.getElementById(input_id).value;
    if(input_value.length < 8){
        document.getElementById(error_name).textContent="Password should be 8 character long!";
    }else{
        document.getElementById(error_name).textContent="";
        isValid();
    }
}

const chkPasswordMatch = (input_id, error_name) => {
    let pword = document.getElementById("pword").value;
    let cword = document.getElementById(input_id).value;
    if (pword != cword) {
        document.getElementById(error_name).textContent="Passwords do not match";
    }else{
        document.getElementById(error_name).textContent="";
        isValid();
    }
}


const isValid = () => {
    let full_name = document.getElementById("full-name").value;
    let email = document.getElementById("email").value;
    let phone = document.getElementById("phone").value;
    let gender = document.getElementById("gender").value;
    let address = document.getElementById("address").value;
    let pword = document.getElementById("pword").value;
    let cword = document.getElementById("cword").value;
    const sign_up = document.getElementById("sign-up");

    document.getElementById('empty-form').textContent="";

    if(!alphabetChecker(full_name)) return false;

    if(!emailChecker(email)) return false;

    if(pword.length < 8) return false;

    if(pword != cword) return false;

    sign_up.disabled = false;
    sign_up.style.cursor="pointer";

    return true;
}


const postFormDataAsJson = async ({url, formData}) => {
    const plainFormData = Object.fromEntries(formData.entries());

    plainFormDataObject = {
        fullName : plainFormData.fullName,
        email: plainFormData.email,
        mobileNumber: plainFormData.mobileNumber,
        address: plainFormData.address,
        gender: plainFormData.gender,
        password:  plainFormData.pword
    }
    const formatDataAsJsonString = JSON.stringify(plainFormDataObject);
    const fetchOptions = {
        method: "POST",
        headers: {
            "Content-Type" : "application/json",
            Accept:"application/json",
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin" : "https://jsminnastore.herokuapp.com/auth/signup",
            "Access-Control-Allow-Methods" : "OPTIONS, POST, GET",
        },
        body: formatDataAsJsonString
    };

    const response = await fetch(url, fetchOptions);

    if(response.ok == false){
        const errorMessage = await response.text();
        throw new Error(errorMessage);
    }
    return response.json();
}

const handleFormSubmit = async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const url = 'https://jsminnastore.herokuapp.com/auth/signup';
    if(isValid()){
    document.getElementById('sign-up').style.display = "none";
    document.getElementById('loader').style.display = "block";
    try{
        const formData = new FormData(form);   
        const responseData = await postFormDataAsJson({url, formData});
        if(responseData.success == true){
            localStorage.clear();
            const currentLoggedIn = {
                    "username" : responseData.payload.fullName,
                    "token" : responseData.payload.token                            
                    };
            localStorage.setItem("currentLoggedIn", JSON.stringify(currentLoggedIn));
            window.location = `index.html?message=${responseData.payload.message}`;
        }
    }catch(error){
        document.getElementById('loader').style.display = "none";
        document.getElementById('sign-up').style.display = "block";
        console.log(error);
    }
    }else{
        document.getElementById('empty-form').textContent="Form cannot be empty";
    }

}
  
const signUpForm = document.getElementById('signup-form');
if(signUpForm){
    signUpForm.addEventListener("submit", handleFormSubmit);
}



const validateLogin = () => {
    let email = document.getElementById("email-login").value;
    let password = document.getElementById("pword-login").value;
    const login = document.getElementById("login");
    if(email == '' && password == ''){
        sign_up.disabled = true;
        sign_up.style.cursor="none";
        return false;
    }
    sign_up.disabled = false;
    sign_up.style.cursor="pointer";
}

const postLoginDataAsJson = async ({url, formData}) => {
    const plainFormData = Object.fromEntries(formData.entries());
    plainFormDataObject = {
        email: plainFormData.email,
        password:  plainFormData.password
    }
    const formatDataAsJsonString = JSON.stringify(plainFormDataObject);
    const fetchOptions = {
        method: "POST",
        headers: {
            "Content-Type" : "application/json",
            Accept:"application/json",
            "Access-Control-Allow-Headers" : "Content-Type",
            "Access-Control-Allow-Origin" : "https://jsminnastore.herokuapp.com/auth/signup",
            "Access-Control-Allow-Methods" : "OPTIONS, POST, GET",
        },
        body: formatDataAsJsonString
    };

    const response = await fetch(url, fetchOptions);

    if(response.ok == false){
        const errorMessage = await response.text();
        throw new Error(errorMessage);
    }
    return response.json();
}

const handleLoginSubmit = async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const url = 'https://jsminnastore.herokuapp.com/auth/login';
    if(validateLogin()){
    document.getElementById('login').style.display = "none";
    document.getElementById('loader').style.display = "block";
    try{
        const formData = new FormData(form);   
        const responseData = await postLoginDataAsJson({url, formData});
        if(responseData.success == true){
            localStorage.clear();
            const currentLoggedIn = {
                    "username" : responseData.payload.fullName,
                    "token" : responseData.payload.token                            
                    };
            localStorage.setItem("currentLoggedIn", JSON.stringify(currentLoggedIn));
            window.location = `index.html?message=${responseData.payload.message}`;
        }
    }catch(error){
        document.getElementById('loader').style.display = "none";
        document.getElementById('login').style.display = "block";
        console.log(error);
    }
    }else{
        document.getElementById('empty-form').textContent="Form cannot be empty";
    }

}

const signInForm = document.getElementById('signin-form');
if(signInForm){
    signUsignInFormpForm.addEventListener("submit", handleLoginSubmit);
}



const logout = () => {
    localStorage.clear();
    window.location = `index.html?message=Logout successful`;
}

// index page script
const params = new URLSearchParams(window.location.search);
if(params.has('message')){
    document.getElementById('auth-success').textContent=`${params.get('message')}`;
}
const authUser = JSON.parse((localStorage.getItem("currentLoggedIn")));
if(authUser != null){
    document.getElementById('unauth-user').style.display="none";
    document.getElementById('auth-user-links').style.display="block";
}else{
    document.getElementById('unauth-user').style.display="block";
    document.getElementById('auth-user-links').style.display="none";
}