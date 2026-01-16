$(document).ready(function(){
    $("#mycarousel").carousel( { interval: 2000 } );
    $("#carouselButton").click(function(){
            if ($("#carouselButton").children("span").hasClass('fa-pause')) {
                $("#mycarousel").carousel('pause');
                $("#carouselButton").children("span").removeClass('fa-pause');
                $("#carouselButton").children("span").addClass('fa-play');
            }
            else if ($("#carouselButton").children("span").hasClass('fa-play')){
                $("#mycarousel").carousel('cycle');
                $("#carouselButton").children("span").removeClass('fa-play');
                $("#carouselButton").children("span").addClass('fa-pause');                    
            }
        });
});

//trigger login modal script
$(document).ready( () => {
    $("#login-modal").click( () => {   
        $("#loginModal").modal('show');
    });
});   



//trigger reserve table modal script
$(document).ready( () => {
    $("#reserveTable").click( () => {
        $("#reserveFormModal").modal();
    });
});