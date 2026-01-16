document.addEventListener('DOMContentLoaded', () => {
    window.addEventListener('scroll', () => {

        checkScroll("resortHeading", "fade-in", 1);
        checkScroll("resortDesc", "fade-up", 1);
        checkScroll("resortBtn", "fade-up", 1);
        checkScroll("resortImg", "zoom", 1);

        checkScroll("scubaHeading", "fade-in");
        checkScroll("scubaDesc", "fade-up");
        checkScroll("scubaBtn", "fade-up");
        checkScroll("scubaImg", "zoom");

        checkScroll("weddingHeading", "fade-in");
        checkScroll("weddingDesc", "fade-up");
        checkScroll("weddingBtn", "fade-up");
        checkScroll("weddingImg", "zoom");

    });

});

function checkScroll(ele, anim, block) {
    elements = document.getElementById(ele)
    var positionFromTop = elements.getBoundingClientRect().top;
    var screenHeight = window.innerHeight;

    if (positionFromTop - screenHeight <= 0) {
        elements.classList.add(anim);
        if(block) {
            elements.style.display = "block";
        }
    }
}