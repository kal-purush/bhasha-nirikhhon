/**
 * Created by kedri on 29/01/2017.
 */
$(function(){

    $('#connectionButton').click(function(){
        var username = $('#username').val();
        var pwd=$('#password').val();
        var params = {username: username, password: pwd};

        $.ajax({
            url: "/auth",
            data: params,
            type: 'POST',
            success:function(data){
                token = data.token;
                console.log(data);
                setCookie('token',token,1);
                if(token){
                    $('#connection').modal('toggle');
                    location.reload();
                }
            }
        })
    })
})