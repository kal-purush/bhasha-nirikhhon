
ScrollReveal().reveal('.mySkills');
ScrollReveal().reveal('.skills');
ScrollReveal().reveal('.skills_2');
ScrollReveal().reveal('.html');
ScrollReveal().reveal('how-eat');
ScrollReveal().reveal('myWork');
ScrollReveal().reveal('nsa');
ScrollReveal().reveal('hover');


$('.nav-link active').click(function() {
    var getElement = $(this).attr('href');
    if ($(getElement).length) {
        var getOffset = $(getElement).offset().top;
        $('html, body').animate({
            scrollTop: getOffset -50
        }, 500);
    }
});