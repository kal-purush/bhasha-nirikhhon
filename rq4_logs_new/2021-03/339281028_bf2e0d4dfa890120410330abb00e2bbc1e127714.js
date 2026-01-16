$(document).ready(function () {
    console.log(sessionStorage.getItem('isLogin'));
        if (sessionStorage.getItem('isLogin')) {
            alert('You are already login');
            document.location.href = '/ClinicLy/';
        }

        console.log("tol");

        var valEmail, valPass;
    
        function getVal() {
            valEmail = $('#inEmail').val();
            valPass = $('#inPass').val();
        }
    
        $('#btnLogin').click(function () {
            getVal();
            if (valEmail == "") {
                alert('Input Your Email First !!');
            } else if (valPass == "") {
                alert('Input Your Passwordd !!');
            } else {
                $.post('/ClinicLy/Login', {
                    page: 'login',
                    email: valEmail,
                    pass: valPass
                }, function (data, status) {
                    var jsonData = null;
                    if(data != "failed")
                        jsonData = JSON.parse(data);
                    if (data === "failed") {
                        alert('Email or Password is wrong !!');
                        $('#inId').focus();
                    } else {
                        sessionStorage.setItem('isLogin', true);
                        sessionStorage.setItem('role', jsonData.nameRole);
                        sessionStorage.setItem('userID',jsonData.idUser)
                        sessionStorage.setItem('dt', data);
                        alert('Login Successfuly');
                        document.location.href = '/ClinicLy/';
                    }
                });
            }
        });
    
    });