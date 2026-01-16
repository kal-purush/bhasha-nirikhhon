$('#yta-menu').before('<div class="yta-mask yta-toggle"></div><div class="yta-art"></div>').prepend('<div class="yta-head yta-bg"><i class="fa fa-2x fa-close yta-toggle"></i></div>');
$(document).on( "click", '.yta-toggle', function() {
    $('html').toggleClass('yta-open');
    return false;
});
$(document).on( "click", '.yta-getsub', function() {
    $(this).next().toggleClass('active');
});