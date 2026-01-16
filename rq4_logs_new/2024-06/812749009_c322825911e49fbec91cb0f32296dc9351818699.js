const slider = document.querySelectorAll('.sliderImg');
const prevBtn = document.getElementById('prevBtn');
const nextBtn = document.getElementById('nextBtn');
let currentSlide = 0;

function hideSlider() {
    slider.forEach(elemnt => elemnt.classList.remove('on'));
}

function showSlider() {
    slider[currentSlide].classList.add('on');
}

function nextBtn() {

}

nextBtn.addEventListener('click', () => console.log('proximo'));

prevBtn.addEventListener('click', () => console.log('anterior'));