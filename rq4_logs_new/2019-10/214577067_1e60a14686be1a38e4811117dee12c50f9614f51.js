/////////// scroll events ///////////

var nav = document.getElementById('land-nav');
var heroContent = document.getElementById('hero-content');
var viewHeight = window.innerHeight;
var scrollSection = 0;

window.addEventListener('scroll', ()=>{
    var background = document.getElementById('');
    var scrollAmount = window.pageYOffset;
    
    if( scrollSection > 0 && scrollAmount < viewHeight*0.2) {
        heroContent.classList.remove('hide');
        scrollSection = 0;
    } else if( scrollSection == 0 && scrollAmount > viewHeight*0.1) {
        heroContent.classList.add('hide');
        scrollSection = 1;
    } else if( scrollSection == 1 && scrollAmount > viewHeight){
        nav.classList.remove('white-nav')
        scrollSection = 2;
    } else if(scrollSection == 2 && scrollAmount > viewHeight*3){
        nav.classList.remove('clear')
    } else if(scrollSection == 2 && scrollAmount < viewHeight*3){
        nav.classList.add('clear')
    }
    
    background.style.backgroundPosition = '0' + ' ' + -scrollAmount*0.3 + 'px'
}, false);

/////////// Menu Btn ///////////

var menuBtn = document.getElementById('menu-btn');
var menu = document.getElementById('menu');

menuBtn.addEventListener("click", menuControl);

function menuControl() {
    menu.classList.toggle('open');
}