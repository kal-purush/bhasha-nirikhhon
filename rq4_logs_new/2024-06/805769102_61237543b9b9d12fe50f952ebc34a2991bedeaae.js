document.addEventListener('DOMContentLoaded', () => {
    const orderButton = document.querySelector('.hero button');

    orderButton.addEventListener('click', () => {
        alert('You Will Be Directed to Dashboard Page');
    });
});

 document.getElementById('navbarToggler').addEventListener('click', function () {
    var navbarSupportedContent = document.getElementById('navbarSupportedContent');
    navbarSupportedContent.classList.toggle('open');
});