menuButton = $('#openMenu');
menuWrapper = $('.menu-wrapper');
mainWrapper = $('.main-wrapper');
wrapperBackground = $('#wrapperBackground');

menuButton.click(function() {
  menuWrapper.attr('id', 'open');
  mainWrapper.attr('id', 'menuOpen');
})

wrapperBackground.click(function() {
  menuWrapper.removeAttr('id', 'open');
  mainWrapper.removeAttr('id', 'menuOpen');
})