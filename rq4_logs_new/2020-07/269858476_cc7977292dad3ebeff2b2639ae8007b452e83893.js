$(document).ready(function(){
    $.ajax({
        url:"https://api.github.com/users/supdpk",
        method:"GET",
        success:function(response){
            console.log(response);
            $(".profile-img img").prop("src",response.avatar_url);

        },
        error:function(response){
            console.log(response);
        }
    })
    $.ajax({
        url:"https://api.linkedin.com/v2/people/belbase",
        method:"GET",
        success:function(response){
            console.log(response);
            $(".profile-img img").prop("src",response.avatar_url);

        },
        error:function(response){
            console.log(response);
        }
    })
});
//https://api.linkedin.com/v2/people/(id:{person ID})