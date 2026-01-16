// Toggle dropdown for language switcher

console.log("The script is working!");

function toggleDropdown(event) {
    event.preventDefault(); // Prevent default behavior
    const dropdown = document.getElementById('languageDropdown');
    toggleVisibility(dropdown, event.target);
}

function toggleMobileDropdown(event) {
    event.preventDefault(); // Prevent default behavior
    const dropdown = document.getElementById('mobileLanguageDropdown');
    toggleVisibility(dropdown, event.target);
}

function toggleVisibility(dropdown, trigger) {
    // Toggle visibility
    dropdown.classList.toggle('visible');

    // Update aria-expanded attribute
    const isExpanded = dropdown.classList.contains('visible');
    trigger.setAttribute('aria-expanded', isExpanded);
}




// Close dropdown when clicking outside
document.addEventListener('click', function (event) {
    const dropdown = document.getElementById('languageDropdown');
    const switcher = document.querySelector('.language-switcher');
    if (!switcher.contains(event.target)) {
        dropdown.style.display = 'none';
    }
});



// Open modal
function openModal() {
    console.log("openModal working");
    const modal = document.getElementById('modal');
    modal.style.display = 'block';
}

// Close modal
function closeModal() {
    console.log("closeModal working");
    const modal = document.getElementById('modal');
    modal.style.display = 'none';
}

// Close modal if user clicks outside the modal content
window.onclick = function(event) {
    console.log("Modal working");
    const modal = document.getElementById('modal');
    if (event.target === modal) {
        modal.style.display = 'none';
    }
};
//reviews
document.addEventListener("DOMContentLoaded", function () {
    const container = document.querySelector(".reviews-container");
    const reviewsSection = document.querySelector("#reviews");

    const maxScrollSpeed = 7; // Maximum scroll speed (adjust as needed)
    let scrollDirection = 0; // Direction of scroll (-1 for left, 1 for right)
    let lastTime = 0; // Time tracking for smooth animation

    // Smooth scrolling function
    function smoothScroll() {
        const currentTime = Date.now();
        const deltaTime = currentTime - lastTime;
        const speed = Math.min(maxScrollSpeed, deltaTime / 10); // Control scroll speed

        if (scrollDirection !== 0) {
            container.scrollLeft += scrollDirection * speed;
        }
        lastTime = currentTime;

        // Call the function on the next frame
        requestAnimationFrame(smoothScroll);
    }

    // Mouse move listener to track cursor position
    reviewsSection.addEventListener("mousemove", (e) => {
        const { left, width } = reviewsSection.getBoundingClientRect();
        const cursorX = e.clientX - left;
        const centerX = width / 2;

        // Calculate scroll direction based on cursor position
        if (cursorX < centerX * 0.3) {
            scrollDirection = -1; // Scroll left
        } else if (cursorX > centerX * 1.7) {
            scrollDirection = 1; // Scroll right
        } else {
            scrollDirection = 0; // No scroll
        }
    });

    // Start the smooth scrolling process
    requestAnimationFrame(smoothScroll);
});

// Update file name display after file selection
function updateFileName() {
    const fileInput = document.getElementById('document');
    const fileName = document.getElementById('fileName');
    fileName.textContent = fileInput.files[0] ? fileInput.files[0].name : 'No file chosen';
}

// Toggle the mobile menu visibility when burger icon is clicked
document.querySelector('.burger-menu').addEventListener('click', toggleBurgerMenu);

function toggleBurgerMenu() {
    const burgerMenu = document.querySelector('.burger-menu');
    const mobileMenu = document.querySelector('.mobile-menu');

    // Toggle active class on both elements
    burgerMenu.classList.toggle('active');
    mobileMenu.classList.toggle('active');
}


// Optional: Close the mobile menu if a link is clicked
document.querySelectorAll('.mobile-menu a').forEach(item => {
    item.addEventListener('click', function() {
        document.querySelector('.mobile-menu').classList.remove('active');
    });
});


document.getElementById("contactForm").addEventListener("submit", function (e) {
    e.preventDefault(); // Prevent default form submission
    alert("Thank you for contacting us! We'll get back to you shortly.");
    this.reset(); // Clear the form fields
});
