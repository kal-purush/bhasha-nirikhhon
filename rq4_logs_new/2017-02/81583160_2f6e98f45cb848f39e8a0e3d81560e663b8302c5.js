// hide these elements on page load
$('#main-nav, .panel').hide();


// close everything when site branding is clicked
$('#branding').on('click', function() {
  $('.panel, #main-nav').hide();
});


// toggle mobile menu on hamburger click
$('.nav-trigger').on('click', function() {
  $('#main-nav').slideToggle( 200 );
});


// nav: open panels on click
$('#main-nav a, #medium-up-nav a').on('click', function() {
  var tabID = $(this).attr('data-tab');
  $('#main-nav').hide();
  $('.panel').hide();
  $('#' + tabID).fadeIn( 300 );
});


// close panels when clicking X
$('.close').on('click', function() {
  $('.panel').hide();
});