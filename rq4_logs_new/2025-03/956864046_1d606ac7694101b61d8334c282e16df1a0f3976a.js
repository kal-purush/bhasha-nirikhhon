//Elasticidad botones menu
$(document).ready(function() {
    smoothScroll.init({
        selector: '[data-scroll]',
        //selector: 'a.nav-link', 
        // 2 seg equivale a 2000)  
        speed: 500,
        //easing: 'easyInCubic'
        //easing: 'easyInOutQuad',
        //easing:'easeInQuart',
        //easing:'easeInQuart',
        easing: 'easeInOutCubic',
    });

});


//desaparece el boton ScrollTop al cargar la pagina web
document.getElementById("flechabtn").style.display = "none";

//esconder botón ScrollTop (parte superior)
window.onscroll = function() {
    scrollFunction()
};


function scrollFunction() {
    if (document.body.scrollTop > 280 || document.documentElement.scrollTop > 280) {
        document.getElementById("flechabtn").style.display = "block";
    } else {
        document.getElementById("flechabtn").style.display = "none";
    }
}




//Funcián Scrolltop (Jquery)
function topFunction() {
    $('a.scroll-top').click(function(event) {
        event.preventDefault();
        $('html, body').animate({ scrollTop: 0 });
    });
}