let slides = document.querySelectorAll(".slide");
let dotsContainer = document.querySelector(".dot-container");
let dots = document.querySelectorAll(".dot-container button");

dotsContainer.addEventListener("click", (e) => {
    if(e.target.tagName.toLowerCase() == "button") {
        dots.forEach(dot => dot.classList.remove("active-dot"));
        e.target.classList.add("active-dot");

        let index = Array.from(dots).findIndex(dot => dot.classList.contains('active-dot'));
        slides.forEach(slide => slide.classList.remove("active"));
        let slidesArray = Array.from(slides);
        slidesArray[index].classList.add("active");
    }
})

function prev() {
    let index = Array.from(slides).findIndex(slide => slide.classList.contains('active'));
    slides.forEach(slide => slide.classList.remove("active"));
    let slidesArray = Array.from(slides);
    console.log(slidesArray)

    if(index <= 0) {
        index = slidesArray.length;
    }
    slidesArray[index - 1].classList.add("active");

    dots.forEach(dot => dot.classList.remove("active-dot"));
    dots[index - 1].classList.add("active-dot");
}



function next() {
    let index = Array.from(slides).findIndex(slide => slide.classList.contains('active'));
    slides.forEach(slide => slide.classList.remove("active"));
    let slidesArray = Array.from(slides);
    console.log(slidesArray)

    if(index >= slidesArray.length - 1) {
        index = -1;
    }
    slidesArray[index + 1].classList.add("active");

    dots.forEach(dot => dot.classList.remove("active-dot"));
    dots[index + 1].classList.add("active-dot");
}