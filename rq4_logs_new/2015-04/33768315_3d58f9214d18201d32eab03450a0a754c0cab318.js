$(document).on('ready', function(){
  $('.toggle-menu').on('click', function(){
    $('#main').toggleClass('open');
  });

  $('.primary a').on('click', function(){
    $('#main').removeClass('open');
  });
});