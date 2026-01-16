$(document).ready(function () {
    $("#bttn").click(function () {
        var name = $("#Nname").val();
        var cname = $("#CName").val();
        var email = $("#Email").val();
        var phone = $("#Phone").val();
        var message = $("#Message").val();
        var x = document.forms["myForm"]["Nname","Email","Phone"].value;
        if (x == "") {
          return false;
        }else{
            $.ajax({
                url: 'mail.php',
                type: 'post',
                dataType: 'html',
                data: {name: name, cname: cname, email: email, phone: phone, message: message},
                beforeSend: functionBefore,
                success: functionSuccess
            });
        }
    });
    function functionBefore(){
        $('#info').text('please wait...');
    }
    function functionSuccess(data){
        
        $('#info').text(data);
    }
});