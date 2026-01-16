// Selecting elements
const bar = document.getElementById('bar');
const close = document.getElementById('close');
const nav = document.getElementsByClassName('header_navbar')[0];
const loginBtn = document.querySelector('.btn-log'); // Login button that opens popup
const closePopupBtn = document.querySelector('.form-popup .close-btn');
const overlay = document.querySelector('.blur-bg-overlay');
const formPopup = document.querySelector('.form-popup');
const loginSignUpLinks = document.querySelectorAll('.form-box .bottom-link a');

// Check if the login button exists before adding the event listener
if (loginBtn) {
    // Show form popup and default to login form
    loginBtn.addEventListener('click', () => {
        document.body.classList.add('show-popup');
        formPopup.classList.remove('show-signup'); // Show login form by default
        if (overlay) overlay.style.display = 'block'; // Show overlay if it exists
    });
}

// Hide form popup
if (closePopupBtn) {
    closePopupBtn.addEventListener('click', () => {
        document.body.classList.remove('show-popup');
        if (overlay) overlay.style.display = 'none'; // Hide overlay if it exists
    });
}

// Toggle between Login and Signup forms
loginSignUpLinks.forEach(link => {
    link.addEventListener("click", (e) => {
        e.preventDefault();
        if (formPopup) formPopup.classList.toggle("show-signup", link.id === "signup-link");
    });
});

// Navbar toggle logic
if (bar) {
    bar.addEventListener('click', () => {
        if (nav) {
            nav.classList.add('active');
            console.log("Navbar opened.");
        } else {
            console.log("Navbar element not found.");
        }
    });
} else {
    console.log("Bar element not found.");
}

if (close) {
    close.addEventListener('click', () => {
        if (nav) {
            nav.classList.remove('active');
            console.log("Navbar closed.");
        } else {
            console.log("Navbar element not found.");
        }
    });
} else {
    console.log("Close element not found.");
}

// Main image and small images logic
const mainimg = document.getElementsByClassName("mainimg")[0];
const smallimg = document.getElementsByClassName("smallimg");

if (mainimg && smallimg.length > 0) {
    Array.from(smallimg).forEach((img, index) => {
        img.onclick = function () {
            mainimg.src = img.src;
        };
    });
} else {
    console.log("Main image or small images not found.");
}