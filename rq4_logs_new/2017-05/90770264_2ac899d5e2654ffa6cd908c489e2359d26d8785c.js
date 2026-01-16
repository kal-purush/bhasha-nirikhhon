$(document).ready(function(){
   
    $('.slider').slider({indicators:false});
    $(".slider").slider("pause");
    $("#next1").click(function(){$(".slider.1").slider("next");});
    $("#next2").click(function(){$(".slider.2").slider("next");});
    $("#next3").click(function(){$(".slider.3").slider("next");});
    $("#next4").click(function(){$(".slider.4").slider("next");});
    $("#next5").click(function(){$(".slider.5").slider("next");});
    $("#next6").click(function(){$(".slider.6").slider("next");});

    $("#next11").click(function(){$(".slider.1").slider("next");});
    $("#prev11").click(function(){$(".slider.1").slider("prev");});
     $("#nclose").click(function(){$(".slider.1").slider("close");});

    $("#prev1").click(function(){$(".slider.1").slider("prev");});
    $("#prev2").click(function(){$(".slider.2").slider("prev");});
    $("#prev3").click(function(){$(".slider.3").slider("prev");});
    $("#prev4").click(function(){$(".slider.4").slider("prev");});
    $("#prev5").click(function(){$(".slider.5").slider("prev");});
    $("#prev6").click(function(){$(".slider.6").slider("prev");});

    /*$(".modal").modal();*/
    $('.modal-trigger').leanModal();
    /*$('#modal1').modal('close');*/

    /*$("#plus").click(function(){$("#modal1").modal("open");});*/
  });