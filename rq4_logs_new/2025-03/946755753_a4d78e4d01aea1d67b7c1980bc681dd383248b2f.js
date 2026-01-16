ocument.addEventListener("DOMContentLoaded", function() {
    let images = document.querySelectorAll(".gallery-container img");
    
    images.forEach(img => {
        img.addEventListener("click", () => { 
            alert("जय महादेव! यह मंदिर की छवि है।");
        });
    });
});
document.querySelectorAll('nav ul li a').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
        e.preventDefault();
        document.querySelector(this.getAttribute('href')).scrollIntoView({
            behavior: 'smooth'
        });
    });
});