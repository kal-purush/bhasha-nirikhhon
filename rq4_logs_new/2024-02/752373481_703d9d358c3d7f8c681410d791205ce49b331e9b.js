$(function(){
    $("#header").load("header.html");
    $("#footer").load("footer.html");
});


document.querySelectorAll('.card-header').forEach((item, index) => {
    item.addEventListener('click', event => {
        document.querySelectorAll('.card-header i').forEach(icon => {
            icon.style.transform = 'rotate(0deg)';
        });
        
        const isOpen = item.getAttribute('aria-expanded') === 'true';
        const arrowIcon = item.querySelector('i'); // Select the first i tag inside the card-header
        
        if (isOpen) {
            arrowIcon.style.transform = 'rotate(0deg)';
        } else {
            arrowIcon.style.transform = 'rotate(180deg)';
        }
    });
});