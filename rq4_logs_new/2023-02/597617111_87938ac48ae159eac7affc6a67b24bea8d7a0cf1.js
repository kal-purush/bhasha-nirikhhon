//button disabled
$('#disabled').attr("disabled",true)


//password attribute
$('#password_validation').on('focus',function(){
   $('.password_validator').slideDown()
})
$('#password_validation').on('blur',function(){
    $('.password_validator').slideUp()
 })

//conpassword attribute
 $('#incorrect_validation').on('focus',function(){
    $('.incorrect_validator').slideDown()
 })
 $('#incorrect_validation').on('blur',function(){
     $('.incorrect_validator').slideUp()
  })

 $('#password_validation').on('keyup',function(){
    passValue = $(this).val();


    if(passValue.match(/[a-z]/g)){
        $('.lowercase').addClass('checked')
    }
    else{
        $('.lowercase').removeClass('checked')
    }
 if(passValue.match(/[A-Z]/g)){
    $('.uppercase').addClass('checked')
}
else{
    $('.uppercase').removeClass('checked')
}
if(passValue.match(/[1-9]/g)){
    $('.number').addClass('checked')
}
else{
    $('.number').removeClass('checked')
}
if(passValue.match(/[!@#$%^&*]/g)){
    $('.special').addClass('checked')
}
else{
    $('.special').removeClass('checked')
}
if(passValue.length >= 8){
    $('.charlength').addClass('checked')
}
else{
    $('.charlength').removeClass('checked')
}
$(document).ready(function(){
    $("#login").keyup(function(){
     if($('#incorrect_validation').val() == $('#password_validation').val()) {
       
        $('.incorrect'). text(" ")
        if(passValue.length >= 8 && passValue.match(/[!@#$%^&*]/g) && passValue.match(/[1-9]/g) && passValue.match(/[A-Z]/g) && passValue.match(/[a-z]/g)){
    
            $('#disabled').attr("disabled",false)
            $("#disabled"). css("background-color","black"); 
        }else{
            $('#disabled').attr("disabled",true)
            $("#disabled"). css("background-color","gray");
        }
        
     }   
    else{
        $('.incorrect'). css("rgb(190, 0, 0)")
        $('.incorrect'). text("Password and confirm password didn't match")
    }
        
    })
   

})


 })