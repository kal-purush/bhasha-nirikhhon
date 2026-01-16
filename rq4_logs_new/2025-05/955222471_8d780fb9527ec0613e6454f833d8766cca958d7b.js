const burger = document.getElementById('burger');
const nav = document.querySelector('.nav');

burger.addEventListener('click', function () {
    this.classList.toggle('active');
    nav.classList.toggle('active');

    if (nav.classList.contains('active')) {
        document.body.style.overflow = 'hidden';
    } else {
        document.body.style.overflow = '';
    }
});

const navLinks = document.querySelectorAll('.nav__link');
navLinks.forEach(link => {
    link.addEventListener('click', function () {
        burger.classList.remove('active');
        nav.classList.remove('active');
        document.body.style.overflow = '';
    });
});

const accordionHeaders = document.querySelectorAll('.accordion-header');

accordionHeaders.forEach(header => {
    header.addEventListener('click', function () {
        const accordionContent = this.nextElementSibling;

        document.querySelectorAll('.accordion-content').forEach(content => {
            if (content !== accordionContent && content.style.maxHeight) {
                content.style.maxHeight = null;
                content.previousElementSibling.classList.remove('active');
            }
        });

        this.classList.toggle('active');
        if (accordionContent.style.maxHeight) {
            accordionContent.style.maxHeight = null;
        } else {
            accordionContent.style.maxHeight = accordionContent.scrollHeight + 'px';
        }
    });
});

accordionHeaders[0].click();


const sliderContainer = document.querySelector('.slider-container');
const slides = document.querySelectorAll('.slide');
const prevBtn = document.querySelector('.slider-prev');
const nextBtn = document.querySelector('.slider-next');
const dots = document.querySelectorAll('.dot');

let currentSlide = 0;
const slideCount = slides.length;

function updateSlider() {
    sliderContainer.style.transform = `translateX(-${currentSlide * 100}%)`;

    dots.forEach((dot, index) => {
        dot.classList.toggle('active', index === currentSlide);
    });
}

function ChangeSlide(direction = 1) {
    currentSlide = (currentSlide + direction + slideCount) % slideCount;
    updateSlider();
}

nextBtn.addEventListener('click', function () {
    ChangeSlide(1);
    startAutoSlide();
});

prevBtn.addEventListener('click', function () {
    ChangeSlide(-1);
    startAutoSlide();
});

dots.forEach((dot, index) => {
    dot.addEventListener('click', function () {
        currentSlide = index;
        updateSlider();
        startAutoSlide();
    });
});

let slideInterval = setInterval(ChangeSlide, 5000);

function startAutoSlide() {
    clearInterval(slideInterval);
    slideInterval = setInterval(ChangeSlide, 5000);
}


const tabBtns = document.querySelectorAll('.tab-btn');
const tabPanes = document.querySelectorAll('.tab-pane');

tabBtns.forEach((btn, index) => {
    btn.addEventListener('click', function () {
        document.querySelector('.tab-btn.active').classList.remove('active');
        document.querySelector('.tab-pane.active').classList.remove('active');
        this.classList.add('active');
        tabPanes[index].classList.add('active');
    });
});

const contactForm = document.getElementById('contact-form');

function validateEmail(email) {
    const reg = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return reg.test(email.toLowerCase());
}

contactForm.addEventListener('submit', function (e) {
    e.preventDefault();

    let isValid = true;

    // Валидация имени
    const nameInput = document.getElementById('name');
    const nameError = nameInput.nextElementSibling;

    if (nameInput.value.trim().length < 2) {
        nameInput.classList.add('error');
        nameError.textContent = 'Имя должно содержать минимум 2 символа';
        nameError.style.display = 'block';
        isValid = false;
    } else {
        nameInput.classList.remove('error');
        nameError.style.display = 'none';
    }

    const emailInput = document.getElementById('email');
    const emailError = emailInput.nextElementSibling;

    if (!validateEmail(emailInput.value)) {
        emailInput.classList.add('error');
        emailError.textContent = 'Введите корректный email';
        emailError.style.display = 'block';
        isValid = false;
    } else {
        emailInput.classList.remove('error');
        emailError.style.display = 'none';
    }

    if (isValid) {
        alert('Форма успешно отправлена! Мы свяжемся с вами в ближайшее время.');
        contactForm.reset();

        contactForm.querySelectorAll('.error-message').forEach(el => {
            el.style.display = 'none';
        });
        contactForm.querySelectorAll('.form-group input').forEach(el => {
            el.classList.remove('error');
        });
    }
});

const inputs = contactForm.querySelectorAll('input');
inputs.forEach(input => {
    input.addEventListener('input', function () {
        this.classList.remove('error');
        const errorMsg = this.nextElementSibling;
        errorMsg.style.display = 'none';
    });
});

const input_tel = document.querySelector('input[type=tel]');
input_tel.addEventListener('keypress', function (e) {
    let length = this.value.length;
    if (!Number(e.key) && e.key != 0 || e.code == "Space") {
        e.preventDefault();
    } else if ((e.key == 8 || e.key == 7) && length == 0) {
        e.preventDefault();
        this.value = "+7";
    } else {
        var num = this.value.replace('+7', '').replace(/\D/g, '').split(/(?=.)/);
        let i = num.length;
        if (0 <= i) num.unshift('+7');
        if (1 <= i) num.splice(1, 0, ' (');
        if (3 <= i) num.splice(5, 0, ') ');
        if (6 <= i) num.splice(9, 0, '-');
        if (8 <= i) num.splice(12, 0, '-');
        this.value = num.join('');
    }
});


document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();

        const targetId = this.getAttribute('href');
        if (targetId === '#') return;

        const targetElement = document.querySelector(targetId);
        if (targetElement) {
            window.scrollTo({
                top: targetElement.offsetTop - 50,
                behavior: 'smooth'
            });
        }
    });
});