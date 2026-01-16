/* Slide section */
$("#b_sign,#b_log").click(function(event){
    console.log()
    if(event.currentTarget.id=="b_sign" && $(".sec-2").css("display")=="none"  
        || event.currentTarget.id=="b_log" && $(".sec-1").css("display")=="none"){
        $(".sec-2").slideToggle("slow")
        $(".sec-1").slideToggle("slow")
    }
})

/* LogIn-SignUp requests */
$("#register").submit(function(){

    $.ajax( {
        url:'/login/register_me',
        type:"POST",
        data:{
            "pseudo": $(this).find("input[name=pseudo]").val(),
            "username": $(this).find("input[name=username]").val(),
            "password": $(this).find("input[name=password]").val()
        },
        
        success: function(data) {    
            if (data.v == false) 
                alert(data.result.message)
            else
                window.location.href = data.result;
        }  
    });

    return false;
});
$("#log").submit(function(){

    $.ajax( {
        url:'/login/log_me',
        type:"POST",
        data:{
            "username": $(this).find("input[name=username]").val(),
            "password": $(this).find("input[name=password]").val()
        },
        
        success: function(data) { 
            if (data.v == false) 
                alert(data.result)
            else
                window.location.href = data.result;
        },
        error:function(data){
            alert(data.statusText+" Bad Username or Password")
        }  
        
    });
    return false;
});