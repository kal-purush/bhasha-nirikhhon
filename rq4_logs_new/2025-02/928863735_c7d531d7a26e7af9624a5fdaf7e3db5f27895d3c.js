// TYPED JS

var typed = new Typed('#text', {
    strings: ['WHEN IMAGINATION MEETS INNOVATION'],
    typeSpeed: 50,
    backSpeed: 50,
    loop: true,
});

// // BRAND SWIPER

// var swiper = new Swiper('.brandSwiper',{
//     slidesPerView: 3,
//     spaceBetween: 10,
//     loop: true,
//     autoplay: true,
//     breakpoints: {
//         1200:{
//             slidesPerView: 4,
//         },
//         900:{
//             slidesPerView: 4,
//         },
//         500:{
//             slidesPerView: 3,
//         },
//     },
// });

// COLLECTION SWIPER

var swiper = new Swiper('.collectionSwiper', {
    slidesPerView: 5, // Adjust based on how many slides to show at a time
    spaceBetween: 20, // Reduced space between sliders for a tighter layout
    loop: true, // Enable looping
    autoplay: {
        delay: 3000, // Faster auto-slide transition
    },
    speed: 500, // Faster slide animation
    navigation: {
        nextEl: '.swiper-button-next',
        prevEl: '.swiper-button-prev',
    },
    pagination: {
        el: '.swiper-pagination',
        clickable: true,
    },
    breakpoints: {
        1200: {
            slidesPerView: 5,
        },
        900: {
            slidesPerView: 2,
        },
        500: {
            slidesPerView: 1,
        },
        400: {
            slidesPerView: 1,
        },
        300: {
            slidesPerView: 1,
        },
    },
});


// TEAM SWIPER

var swiper = new Swiper('.teamSwiper', {
    slidesPerView: 4,
    spaceBetween: 10,
    loop: true,
    autoplay: true,
    breakpoints: {
        1200: {
            slidesPerView: 4,
        },
        900: {
            slidesPerView: 2,
        },
        500: {
            slidesPerView: 1,
        },
        400: {
            slidesPerView: 1,
        },
        300: {
            slidesPerView: 1,
        },
    },
});

// TESTIMONIAL SWIPER

var swiper = new Swiper('.testSwiper', {
    slidesPerView: 1,
    spaceBetween: 10,
    loop: true,
    autoplay: true,
});

// SHOW MENU

let bar = document.querySelector('.bars');
let menu = document.querySelector('.menu');

bar.addEventListener('click', () => {
    menu.classList.toggle('show_menu');
});



// Custom cursor

const cursor = document.querySelector("[data-cursor]");
const hoverElements = [...document.querySelectorAll("a"), ...document.querySelectorAll("button")];

const cursorMove = function (event) {
    cursor.style.top = `${event.clientY}px`;
    cursor.style.left = `${event.clientX}px`;
}

window.addEventListener("mousemove", cursorMove);

// timer section js

// Set the target date for the countdown
var countDownDate = new Date("2025-02-09T00:00:00").getTime(); // Replace with your desired date

// Update the countdown every 1 second
var x = setInterval(function () {

    // Get the current date and time
    var now = new Date().getTime();

    // Calculate the remaining time
    var distance = countDownDate - now;

    // Calculate days, hours, minutes, and seconds
    var days = Math.floor(distance / (1000 * 60 * 60 * 24));
    var hours = Math.floor((distance % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    var minutes = Math.floor((distance % (1000 * 60 * 60)) / (1000 * 60));
    var seconds = Math.floor((distance % (1000 * 60)) / 1000);

    // const header = document.getElementById('Header');

    // Update the countdown on the webpage
    document.getElementById("days").innerHTML = (days < 10 ? "0" + days : days);
    document.getElementById("hours").innerHTML = (hours < 10 ? "0" + hours : hours);
    document.getElementById("minutes").innerHTML = (minutes < 10 ? "0" + minutes : minutes);
    document.getElementById("seconds").innerHTML = (seconds < 10 ? "0" + seconds : seconds);

    // When the countdown is over, display "Event Started!"
    if (distance < 0) {
        clearInterval(x); // Stop the countdown
        document.getElementById("countdown-timer").style.display = "none"; // Hide the timer
        document.getElementById("event-started").style.display = "block"; // Show "Event Started"
    }

}, 1000);


// JavaScript to toggle the visibility of the "Back to Top" button
const backToTopButton = document.getElementById('back-to-top');

// Show/hide the button based on scroll position
window.addEventListener('scroll', () => {
    if (window.scrollY > 200) {
        backToTopButton.classList.add('active');
    } else {
        backToTopButton.classList.remove('active');
    }
});

// Smooth scroll to top when the button is clicked
backToTopButton.addEventListener('click', (event) => {
    event.preventDefault();
    window.scrollTo({
        top: 0,
        behavior: 'smooth'
    });
});


// Select the navigation bar
const nav = document.getElementById('main-nav');

// Add a scroll event listener to toggle the sticky class
window.addEventListener('scroll', () => {
    if (window.scrollY > 150) {
        nav.classList.add('sticky');
    } else {
        nav.classList.remove('sticky');
    }
});