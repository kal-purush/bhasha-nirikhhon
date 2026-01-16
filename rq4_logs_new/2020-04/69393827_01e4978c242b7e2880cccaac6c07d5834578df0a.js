import EventsBase from './events_base';

$(window).scroll(function() {
  if ($(this).scrollTop() > 600) {
    $('.floating-buy-button').addClass('show-buy-button');
  } else {
    $('.floating-buy-button').removeClass('show-buy-button');
  }
});