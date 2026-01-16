function getVerificationForgetPassWord(e) {

    var email = $("#email").val();
    var emailTest = /^[\w\-\.]+@[a-z0-9]+(\-[a-z0-9]+)?(\.[a-z0-9]+(\-[a-z0-9]+)?)*\.[a-z]{2,4}$/i;
    if(!emailTest.test(email)){
        showInfoModel("请输入合法的邮箱地址");
        return
    }
    $.ajax({
        url: url+"/user/getVerification",
        type:"post",
        dataType: 'json',
        data:{
            email:email
        },
        success: function(res){
            console.log(res)
            if(res.request == "SUCCESS"){
                var i = 59;
                var time = setInterval(function () {
                    $(e).html(i+"秒后重新发送");
                    $(e).attr("disabled",true);
                    i--;
                    if(i < 0){
                        $(e).html("发送验证码");
                        $(e).attr("disabled",false);
                        clearInterval(time);
                    }
                },1000);
            }else{
                console.log(res);
            }
        },
        error:function(res){
            window.location.href = "../pages/404.html";
        }
    });

}
function  fogetPassword() {
   var account =  $("#account").val();
   var email =  $("#email").val();
   var verification =  $("#verification").val();

   if(account == ""){
       showInfoModel("请输入账号信息");
       return;
   }
    if(email == ""){
        showInfoModel("请输入邮箱信息");
        return;
    }
    if(verification == ""){
        showInfoModel("请输入验证码");
        return;
    }
    $.ajax({
        url: url+"/user/fogetPassWord",
        type:"post",
        dataType: 'json',
        data:{
            account:account,
            userVerification:verification
        },
        success: function(res){
            if(res.request == "success"){
               showInfoModel(res.msg);
               setTimeout(function () {
                   window.location.href = "../pages/login.html";
               },3000);
            }else{
                showInfoModel(res.msg);
            }
        },
        error:function(res){
            window.location.href = "../pages/404.html";
        }
    });
}