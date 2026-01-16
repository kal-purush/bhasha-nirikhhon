document.addEventListener('DOMContentLoaded', () => {
    console.log('Ready');

    const registration_form = document.forms['registration-form'];

    const name = registration_form['name'];
    const email = registration_form['email'];
    const username = registration_form['username'];
    const password = registration_form['password'];
    const cpassword = registration_form['cpassword'];
    const genders = registration_form['gender'];
    const dob = registration_form['dob'];
    const registration_btn = registration_form['registration_btn'];

    // console.log(email, username, password, cpassword, gender, dob, registration_btn);
    // console.log(gender);

    name.addEventListener('keyup', (e) => {
        // console.log(e.target.value);
        e.preventDefault();
        validate_name(e.target.value.trim());
    });

    email.addEventListener('keyup', (e) => {
        // console.log(e.target.value);
        e.preventDefault();
        validate_email(e.target.value.trim());
    });

    username.addEventListener('keyup', (e) => {
        // console.log(e.target.value);
        e.preventDefault();
        validate_username(e.target.value.trim());
    });

    password.addEventListener('keyup', (e) => {
        // console.log(e.target.value);
        e.preventDefault();
        validate_password(e.target.value.trim());
    });

    cpassword.addEventListener('keyup', (e) => {
        // console.log(e.target.value);
        e.preventDefault();
        validate_cpassword(e.target.value.trim(), password.value.trim());
    });

    genders.forEach((gender) => {
        // console.log(e);
        gender.addEventListener('change', (e) => {
            // console.log(e.target.value);
            e.preventDefault();
            validate_gender(e.target.value.trim());
        });
    });

    dob.addEventListener('change', (e) => {
        // console.log(e.target.value);
        e.preventDefault();
        validate_dob(e.target.value.trim());
    });

    document.addEventListener('submit', (e) => {
        e.preventDefault();
        // console.log();
        // return;

        validate_name(name.value.trim());
        validate_email(email.value.trim());
        validate_username(username.value.trim());
        validate_password(password.value.trim());
        validate_cpassword(cpassword.value.trim(), password.value.trim());

        validate_gender(genders[0].value.trim());
        validate_gender(genders[1].value.trim());
        validate_gender(genders[2].value.trim());

        validate_dob(dob.value.trim());

        if (
            validate_name(name.value.trim()) &&
            validate_email(email.value.trim()) &&
            validate_username(username.value.trim()) &&
            validate_password(password.value.trim()) &&
            validate_cpassword(cpassword.value.trim(), password.value.trim()) &&
            (
                validate_gender(genders[0].value.trim()) ||
                validate_gender(genders[1].value.trim()) ||
                validate_gender(genders[2].value.trim())
            ) &&
            validate_dob(dob.value.trim())
        ) {
            e.target.submit();
        }
    });
});

const validate_name = (name) => {

    const err_name = document.getElementById('err_name');

    if (name.length === 0) {
        err_name.innerText = "Name is required";
    } else if (name.length < 2) {
        err_name.innerText = "Name must be greater than 2 character";
    } else if (!name.match(/^[a-zA-Z-.]/g)) {
        err_name.innerText = "Name must be contains alpha character, (.) and (-)";
    } else {
        err_name.innerText = "";
        return true;
    }

    return false;
}

const validate_email = (email) => {

    const err_email = document.getElementById('err_email');

    if (email.length === 0) {
        err_email.innerText = "Email is required";
    } else if (!email.match(/^[a-zA-Z0-9._-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+(?:\.[a-zA-Z0-9-]+)*$/g)) {
        err_email.innerText = "Email is not valid";
    } else {
        err_email.innerText = "";
        return true;
    }

    return false;
}

const validate_username = (username) => {

    const err_username = document.getElementById('err_username');

    if (username.length === 0) {
        err_username.innerText = "User Name is required";
    } else if (username.length < 2) {
        err_username.innerText = "User Name must be lager than 2 character";
    } else if (!username.match(/^([a-zA-z0-9_-]*)$/g)) {
        err_username.innerText = "User Name must be alphanumeric, dash (-) and Underscore (_)";
    } else {
        err_username.innerText = "";
        return true;
    }

    return false;
}

const validate_password = (password) => {

    const err_password = document.getElementById('err_password');

    if (password.length === 0) {
        err_password.innerText = "Password is required";
    } else if (password.length < 8) {
        err_password.innerText = "Password must be 8 characters or greater";
    } else if (!password.match(/[@#$%]+/g)) {
        err_password.innerText = "Password must include special characters (@ # $ %)";
    } else {
        err_password.innerText = "";
        return true;
    }

    return false;
}

const validate_cpassword = (cpassword, password) => {

    const err_cpassword = document.getElementById('err_cpassword');

    if (cpassword.length === 0) {
        err_cpassword.innerText = "Confirm Password is required";
    } else if (cpassword !== password) {
        err_cpassword.innerText = "Confirm Password must equal to Password";
    } else {
        err_cpassword.innerText = "";
        return true;
    }

    return false;
}

const validate_gender = (gender) => {

    const err_gender = document.getElementById('err_gender');

    if (!gender.checked) {
        err_gender.innerText = "Gender is required";
    } else if (!gender.match(/(male|female|other)/g)) {
        err_gender.innerText = "Gender is not valid";
    } else {
        err_gender.innerText = "";
        return true;
    }

    return false;
}

const validate_dob = (dob) => {

    const err_dob = document.getElementById('err_dob');

    if (dob.length === 0) {
        err_dob.innerText = "Date of birth is required";
    } else if (!dob.match(/^\d{4}-\d{2}-\d{2}$/g)) {
        err_dob.innerText = "Date of birth is not valid";
    } else {
        err_dob.innerText = "";
        return true;
    }

    return false;
}