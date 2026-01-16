var n = 1;
var buttonLeft = document.getElementsByClassName('prev')[0];
var buttonRight = document.getElementsByClassName('next')[0];
var slider = document.getElementsByClassName('slider')[0];

var elem;

function nexSlide(){
    
    slider.classList.add('slide-left-out')
    slider.children[slider.children.length - 1].classList.add('slideOutRight');
    elem.classList.add('slideOutLeft');
    slider.insertBefore(elem, slider.children[0]);
    setTimeout(()=>{
        slider.removeChild(slider.children[slider.children.length - 1]);
        slider.classList.remove('slide-left-out');
        slider.classList.add('slide-right-in');
        elem.classList.remove('slideOutLeft');
    },300);
    setTimeout(()=>{
        slider.classList.add('end-left-anim');
        slider.classList.remove('slide-right-in');
    },600);
    setTimeout(()=>{
        slider.classList.remove('end-left-anim');
    },900);
}

function prevSlide(){

    slider.classList.add('slide-right-out')
    slider.children[0].classList.add('slideOutLeft');
    elem.classList.add('slideOutRight');
    slider.insertBefore(elem, slider.children[slider.children.length ]);
    setTimeout(()=>{
        slider.removeChild(slider.children[0]);
        slider.classList.remove('slide-right-out');
        slider.classList.add('slide-left-in');
        elem.classList.remove('slideOutRight');
    },300);
    setTimeout(()=>{
        slider.classList.add('end-right-anim');
        slider.classList.remove('slide-left-in');
    },600);
    setTimeout(()=>{
        slider.classList.remove('end-right-anim');
    },900);
}

function generateElem(){
    n = n == 1 ? 2 : 1;
    elem = document.createElement('div');
    elem.className = 'slide';
    elem.innerHTML = '<div class="slide-person pers_' + n + '"></div>';
}

buttonLeft.addEventListener( "click" , function() {
    generateElem();
    prevSlide();
});
buttonRight.addEventListener( "click" , function() {
    generateElem();
    nexSlide();
});