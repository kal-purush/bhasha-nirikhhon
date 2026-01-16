var hamburger = document.getElementsByClassName('hamburger')[0];
hamburger.addEventListener("click", function() {
    hamburger.classList.toggle("menuOpen");
    mobile_nav = document.getElementsByClassName('mobile-nav')[0];
    $(document.body).toggleClass('blur');
    $(mobile_nav).toggleClass('menuOpen');
});
$('.mobile-nav a').on('click', function () {
    hamburger.classList.toggle("menuOpen");
    mobile_nav = document.getElementsByClassName('mobile-nav')[0];
    $(document.body).toggleClass('blur');
    $(mobile_nav).toggleClass('menuOpen');
});

$('.experience-button').on('click', function(event){
    event.stopPropagation();
    event.stopImmediatePropagation();
    all_buttons = document.getElementById('experience-section').getElementsByTagName('button');
    experience_detail_divs = document.getElementsByClassName('details-experience');
    for(i = 0; i < all_buttons.length; i++) {
        if (all_buttons[i] !== this) {
            $(all_buttons[i]).removeClass('active');
            $(experience_detail_divs[i]).removeClass('active');
        } else {
            $(experience_detail_divs[i]).addClass('active');
        }
    }
    $(this).addClass('active');
});

$(document).ready(function(){
    $("a").on('click', function(event) {
        if (this.hash !== "") {
            event.preventDefault();
            var hash = this.hash;
            $('html, body').animate({
                scrollTop: $(hash).offset().top
            }, 800, function(){
                window.location.hash = hash;
            });
        }
    });
});

$(document).ready(function () {
    var lastScrollTop = 0;
    $(window).scroll(function () {
        var st = $(this).scrollTop();
        header = document.getElementsByTagName('header')[0];
        if (st === 0) {
            $(header).css('height', '100px');
            $(header).css('box-shadow', 'none');
            $(header).css('transform', 'translateY(0px)');
        } else if (st > lastScrollTop){
            $(header).css('height', '70px');
            $(header).css('box-shadow', 'none');
            $(header).css('transform', 'translateY(-70px)');
        } else if (st < lastScrollTop) {
            $(header).css('transform', 'translateY(0)');
            $(header).css('height', '70px');
            $(header).css('box-shadow', '0 10px 30px -10px rgba(2, 12, 27, 0.7)');
        }
        lastScrollTop = st;
    });
});