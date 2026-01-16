$(function(){
    var pages = ['index', 'About', 'contact'];
    var pathname = window.location.pathname;

    $('.nav-link').each(function(i){
        console.log(pathname)
        console.log(i)
        if(pathname.includes(pages[i])){
            $(this).addClass('active');
            $(this).attr('aria-current', 'page');
        }else if(this.className.includes('active')){
            $(this).removeClass('active');
        }
    });
});