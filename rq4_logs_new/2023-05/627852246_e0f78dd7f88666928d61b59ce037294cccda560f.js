const { ipcRenderer } = require('electron');
const config = require('../../../../config/app');
const fs = require('fs');
const macAddFilePath = path.join(__dirname, '/../../../mac.txt');
const hDSFilePath = path.join(__dirname, '/../../../hds.txt');

$("#register-btn").on("click", function (e) {
    if ($("#confrim-password-div").is(":visible")) {
        $("#register-btn").html('Register');
        $("#login-btn").html('Login');
        $("#confrim-password-div").hide('fade');
    }
    else {
        $("#register-btn").html('Login');
        $("#login-btn").html('Register');
        $("#confrim-password-div").show('fade');
    }
});

$("#login-btn").on("click", function (e) {
    let page = 'login';
    if ($("#confrim-password-div").is(":visible")) {
        page = 'register';
    }
    let email = $('#email').val();
    let pass = $('#password').val();
    if (email == '' || pass == '') {
        $('.alert-danger').html('E-mail and Password is required!');
        $('.alert-success').hide();
        $('.alert-danger').show();
        return;
    }

    if (page == 'login') {
        
        fetch(config.api_url+'/api/login',
        {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                email: email,
                password: pass,
                mac_address: fs.readFileSync(macAddFilePath, 'utf8'),
                hard_disk_serial: fs.readFileSync(hDSFilePath, 'utf8'),
            })
        })
        .then(response => {
            if (response.ok) {
                response.json().then(json => {
                    if(json.body.verified == true){
                        $('.alert-success').html(json.body.message);
                        $('.alert-danger').hide();
                        $('.alert-success').show();
                        const data = {
                            id:json.body.user.id,
                            email:json.body.user.email,
                            token:json.body.token,
                        };
                        ipcRenderer.send('insertToTable', {
                            tableName: process.env.USER_TABLE,
                            data: data,
                        });
                        setTimeout(() => {
                            ipcRenderer.send('relaunch');
                        }, 3000);
                        
                    }
                    else{
                        $('.alert-danger').html(json.body.message);
                        $('.alert-success').hide();
                        $('.alert-danger').show();
                    }
                });
            }
            else{
                $('.alert-danger').html('Something went wrong');
                $('.alert-success').hide();
                $('.alert-danger').show();
            }
        })
        .catch((error) => {
            console.error(error);
        });
    }
    else {
        let confPass = $('#confrim-password').val();
        if (confPass == '') {
            $('.alert-danger').html('Confirm Password is required!');
            $('.alert-success').hide();
            $('.alert-danger').show();
            return;
        }
        if (confPass != pass) {
            $('.alert-danger').html('Passwrod not matched');
            $('.alert-success').hide();
            $('.alert-danger').show();
            return;
        }
        fetch(config.api_url+'/api/signup',
        {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                email: email,
                password: pass,
                mac_address: fs.readFileSync(macAddFilePath, 'utf8'),
                hard_disk_serial: fs.readFileSync(hDSFilePath, 'utf8'),
            })
        })
        .then(response => {
            if (response.ok) {
                response.json().then(json => {
                    $('.alert-success').html(json.body.message);
                    $('.alert-danger').hide();
                    $('.alert-success').show();
                });
            }
            else{
                response.json().then(json => {
                    if(json.body.errors){
                        if(json.body.errors.email){
                            $('.alert-danger').html(json.body.errors.email[0]);
                        }
                        else if(json.body.errors.password){
                            $('.alert-danger').html(json.body.errors.password[0]);
                        }
                        else if(json.body.errors.mac_address){
                            $('.alert-danger').html(json.body.errors.mac_address[0]);
                        }
                        else if(json.body.errors.hard_disk_serial){
                            $('.alert-danger').html(json.body.errors.hard_disk_serial[0]);
                        }
                        else{
                            $('.alert-danger').html('Something went wrong');
                        }
                    }
                    $('.alert-success').hide();
                    $('.alert-danger').show();
                });
            }
        })
        .catch((error) => {
            console.error(error);
        });
    }
});