//Sidenav
$('#menu-button').click(function(e){
    e.stopPropagation();
     $('#hide-menu').toggleClass('show-menu');
     $('#overlay').removeClass('d-none');
});
$('#hide-menu').click(function(e){
    e.stopPropagation();
});
$('body,html,#link-close').click(function(e){
       $('#hide-menu').removeClass('show-menu');
       $('#overlay').addClass('d-none');
});

/* Tooltip */
$(function () {
  $('[data-toggle="tooltip"]').tooltip()
});

/*$('.more-info').onclick(function(){
    $('[data-toggle="tooltip"]').tooltip();
});*/