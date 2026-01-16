var $mapp = $('.mapp');

$('.mapp').waypoint(function (direction) {
  $('.mapp').addClass('js-mapp-animate');
}, { offset: '20%'});