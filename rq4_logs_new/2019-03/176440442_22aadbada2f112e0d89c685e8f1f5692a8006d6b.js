$(document).ready(function(){
    $('.menuToggle').on('click', function(){
        $(this).toggleClass('open');
        $('.topNav').toggleClass('open');
    });
});