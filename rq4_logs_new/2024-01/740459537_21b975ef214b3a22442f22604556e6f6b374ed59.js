const faqItems = document.querySelectorAll('.faq-items');

for (let i = 0; i < faqItems.length; i++) {
    faqItems[i].addEventListener('click', function () {
        this.classList.toggle('active');
    });
}