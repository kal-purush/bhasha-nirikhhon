$(function(){
    var $name = $('#name'),
        $namespan = $('#namespan'),
        $namewarn = $('#namewarn'),
        $tel = $('#tel'),
        $telspan = $('#telspan'),
        $pwd = $('#pwd'),
        $pwdspan = $('#pwdspan'),
        $pwdwarn = $('#pwdwarn')
        $pass = $('#pass'),
        $passbtn = $('#passbtn'),
        $registerbtn = $('#registerbtn'),
        $inp = $('input'),
        num = 60,
        timer = '';

    // 用户名和密码聚焦会有提示信息
    $inp.focus(function(e){
        var id = $(e.target).attr('id');
        switch(id){
            case 'name':
                $namewarn.css('display','block');
                $namespan.css('display','none');
                $namewarn.html('设置后不可更改，最长14个英文或7个汉字');
                break;
            case 'pwd':
                $pwdwarn.css('display','block');
                $pwdspan.css('display','none');
                $pwdwarn.html('长度为8-14个字符 字母/数字/标点符号至少两种 不能有空格和汉字');
        }
    })
    // 用户名和密码失去焦点提示信息消失
    $inp.blur(function(e){
        var id = $(e.target).attr('id');
        switch(id){
            case 'name':
                $namewarn.css('display','none');
                $namespan.css('display','block');
                $namewarn.html('');
                break;
            case 'pwd':
                $pwdwarn.css('display','none');
                $pwdspan.css('display','block');
                $pwdwarn.html('');
        }
    })
    // 用户名数据校验
    $name.change(function(){
        if(!(/^[A-Za-z0-9_\-\u4e00-\u9fa5]+$/.test($name.val()) 
            && /^.*[^\d].*$/.test($name.val()))
            && $name.val() !== ''){
            $namespan.addClass('error');
            $namespan.html('用户名仅支持中英文、数字和下划线且不能为纯数字')
        }else if(!/^[\u4e00-\u9fa5]{1,7}$|^[\dA-Za-z_]{1,14}$/.test($name.val())){
            $namespan.addClass('error');
            $namespan.html('用户名不能超过7个汉字或者14个字符')
        }else{
            $namespan.html('');
        }
    });
    // 手机号数据校验
    $tel.change(function(){
        if(!/^1[3456789]\d{9}$/.test($tel.val())
            && $tel.val() !== ''){
            $telspan.addClass('error')
            $telspan.html('手机号码格式不正确')
        }else{
            $telspan.html('')
        }
    });
    // 密码数据校验
    $pwd.change(function(){
        if(!(/(?!^[0-9]+$)(?!^[A-z]+$)(?!^[^A-z0-9]+$)^[^\s\u4e00-\u9fa5]{8,14}$/.test($pwd.val()))){
            $pwdspan.addClass('error')
            $pwdspan.html('密码设置不符合要求')
        }else{
            $pwdspan.html('')
        }
    });
    // 点击获取验证码，按钮不可用，并出现动态倒计时
    $passbtn.click(function(){
        if($tel.val() === ''){
            $telspan.addClass('error')
            $telspan.html('请您输入手机号码')
        }else{
            $passbtn.attr('disabled','false');
            timer=setInterval(function(){
                num--;
                if(num === 0){
                    clearInterval(timer);
                    $passbtn.val('获取验证码');
                    $passbtn.removeAttr('disabled');
                    num = 60;
                }else{
                    $passbtn.val('重发验证（'+num+'s）');
                }
            }, 1000);
        }
    })
    // 勾选用户协议,并且表单信息均填写完整 注册按钮变为可点击
    $('#choose').on('change', function(){
        if($('#choose:checked').val()) {
            if( $name.val() 
                && $tel.val()
                && $pwd.val()
                && $pass.val()
                && ($pass.val().length===6)
                && !$namespan.html()
                && !$telspan.html()
                && !$pwdspan.html()
                
            ){
                $registerbtn.removeAttr('disabled');
                $registerbtn.css('background','#3f89ec')
            }
        }else{
            $registerbtn.attr('disabled','false');
            $registerbtn.css('background','#bdcefc')
        }
    })
    $registerbtn.click(function(){
        alert("successful")
    })
});