$(function(){
    $("#LoginForm").submit(function(event) {

        event.preventDefault();

        var $form = $(this),
            emailInput = $form.find("input[name='Email']").val(),
            passwordInput = $form.find("input[name='Password']").val();

        var posting =  $.post("login/login", {email: emailInput, password: passwordInput});
            
        posting.done(function(data){

            if(data.startsWith("Redirect")) {
                window.location.href = data.substring(9, data.length);
            } else {
                $("#LoginStatus").html(data);
            }                    
        });
    });     
});