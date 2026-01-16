var autoSlideIndex = 0;
autoShowSlides();

function autoShowSlides() {
    var i;
    var slides = document.getElementsByClassName("mySlides");
    for (i = 0; i < slides.length; i++) {
        slides[i].style.display = "none"; 
    }
    autoSlideIndex++;
    if (autoSlideIndex > slides.length) {autoSlideIndex = 1} 
    slides[autoSlideIndex-1].style.display = "block"; 
    setTimeout(autoShowSlides, 3000); 
}