$(document).ready(function(){
    $("#done").click(function(){
        $("#messageShow").hide();//скрывает текст
        var email = $("#email").val();//принимает инпуты которые нап внизу
        var fail = "";
        var password = document.querySelector('#password').value;//js взятияя знач инпута
        if(email == "") 
            fail = "Повторите ваш email";
        else if(password.trim() == "")//trim() - вырезает пробелы по бокам
            fail = "Не отправлен, введите пароль";
        else if(fail != ""){
        $('#messageShow').html (fail + "<div class='clear'><br></div>");
        $('#messageShow').show();
        return false;
        }

        $.ajax({
            url: '../../api/auth.php',
            type: 'POST',
            cache: false,
            data: {'email': email, 'password': password},
            dataType: 'html',
            beforeSend: function(){
                $('#messageShow').html("Идет загрузка...<div class='clear'><br></div>");
                $('#messageShow').show();
            },
            success: function(data){
                $('#messageShow').html (data + "<div class='clear'><br></div>");
                $('#messageShow').show();
            },
            error: function(k){
                console.log(k);
                $('#messageShow').html(k + "<div class='clear'><br></div>");
            },
        });
    });
});