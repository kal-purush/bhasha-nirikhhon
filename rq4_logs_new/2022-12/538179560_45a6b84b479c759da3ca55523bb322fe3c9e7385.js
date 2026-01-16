var url = window.location.origin+'/check'

function checkCanRegister() {
    if ($('#checkusername').attr('data-check') == 'true' &&
        $('#checkemail').attr('data-check') == 'true' &&
        $('#checkpassword').attr('data-check') == 'true' &&
        $('#checkconfirmpassword').attr('data-check') == 'true'
    ) {
        $("#signup").prop("disabled", false);
    } else {
        $("#signup").prop("disabled", true);
    }
}

$("#username").change(function() {
    var regex = /^[A-Za-z0-9_]*$/;
    const username = $(this).val();
    if (regex.test(username)) {
        $.ajax({
            type: 'GET',
            url: url + '?username=' + username,
            beforeSend: function() {
                $('#checkusername').html('Checking username!');
            },
            success: function(data) {
                if (data.isAvailable) {
                    $('#checkusername').html(
                        '<span class="text-green-500">The username is available.</span>');
                    $('#checkusername').attr('data-check', 'true');
                } else {
                    $('#checkusername').html(
                        '<span class="text-red-500">The username is not available.</span>');
                    $('#checkusername').attr('data-check', 'false');
                }
                checkCanRegister()
            },
            error: function() {
                console.log(data);
            }
        });
    } else {
        $('#checkusername').html(
            '<span class="text-red-500">The username contains invalid characters. Only letters, numbers, periods, and underscores are allowed.</span>'
        );
    }
});

$("#email").change(function() {
    var regex = /\S+@\S+\.\S+/;
    const email = $(this).val();
    if (regex.test(email)) {
        $.ajax({
            type: 'GET',
            url: url + '?email=' + email,
            beforeSend: function() {
                $('#checkemail').html('Checking email!');
            },
            success: function(data) {
                if (data.isAvailable) {
                    $('#checkemail').html(
                        '<span class="text-green-500">The email is available.</span>');
                    $('#checkemail').attr('data-check', 'true');
                } else {
                    $('#checkemail').html(
                        '<span class="text-red-500">The email is not available.</span>');
                    $('#checkemail').attr('data-check', 'false');
                }
                checkCanRegister()
            },
            error: function() {
                console.log(data);
            }
        });
    } else {
        $('#checkemail').html(
            '<span class="text-red-500">The email format is incorrect.</span>');
    }
});

$("#password").change(function() {
    var regex = /^(?=.*\d).{8,}$/;
    const password = $(this).val();
    if (regex.test(password)) {
        $('#checkpassword').html('');
        $('#checkpassword').attr('data-check', 'true');
    } else {
        $('#checkpassword').html(
            "<span class='text-red-500'>A password should contain more than 8 characters with at least 1 letter and number.</span>"
        );
        $('#checkpassword').attr('data-check', 'false');
    }
    checkCanRegister()
});

$("#confirmpassword").change(function() {
    const password = $("#password").val();
    const confirmpassword = $(this).val();
    if (password == confirmpassword) {
        $('#checkconfirmpassword').html('');
        $('#checkconfirmpassword').attr('data-check', 'true');
    } else {
        $('#checkconfirmpassword').html(
            "<span class='text-red-500'>The password doesn't match.</span>");
        $('#checkconfirmpassword').attr('data-check', 'false');
    }
    checkCanRegister()
});