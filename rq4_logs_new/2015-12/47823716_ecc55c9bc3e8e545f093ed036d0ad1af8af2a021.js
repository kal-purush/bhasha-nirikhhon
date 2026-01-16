(function() {
    var logout = function() {
            $('.page').hide();
            $('.nav.pages').hide();
            $('.login.page').show();

            $('span.username').text('');

            $('li.active').removeClass('active');
    };
    $('.logout').click(logout);
})();
