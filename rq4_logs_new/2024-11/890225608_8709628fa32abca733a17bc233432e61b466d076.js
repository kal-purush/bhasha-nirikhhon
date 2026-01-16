document.querySelectorAll('.profile-card').forEach(function(card) {
    card.addEventListener('click', function() {
        alert('More info about this member coming soon!');
    });
});


// JavaScript for carousel
        let currentSlide = 0;
        const slides = document.querySelectorAll('.carousel img');
        const carousel = document.getElementById('carousel');
        let autoSlideInterval;

        function showSlide(index) {
            slides[currentSlide].classList.remove('active');
            currentSlide = (index + slides.length) % slides.length;
            slides[currentSlide].classList.add('active');
        }

        function nextSlide() {
            showSlide(currentSlide + 1);
        }

        function prevSlide() {
            showSlide(currentSlide - 1);
        }

        function startAutoSlide() {
            autoSlideInterval = setInterval(nextSlide, 3000); // Change every 5 seconds
        }

        function stopAutoSlide() {
            clearInterval(autoSlideInterval);
        }

        // Swipe functionality for mobile
        let startX = 0;
        let endX = 0;

        carousel.addEventListener('touchstart', (e) => {
            startX = e.touches[0].clientX;
            stopAutoSlide();
        });

        carousel.addEventListener('touchmove', (e) => {
            endX = e.touches[0].clientX;
        });

        carousel.addEventListener('touchend', () => {
            const threshold = 50; // Minimum swipe distance
            if (startX - endX > threshold) {
                nextSlide();
            } else if (endX - startX > threshold) {
                prevSlide();
            }
            startAutoSlide();
        });

        // Initialize
        startAutoSlide();
		
function validateForm() {
    var name = document.getElementById("name").value;
    var email = document.getElementById("email").value;
	var gender = document.getElementById("gender").value;
	var phone = document.getElementById("phone").value;
    var feedback = document.getElementById("feedback").value;

    if (name == "" || email == "" || gender == "" || phone == "" ||feedback == "") {
        alert("All fields are required.");
        return false;
    }

    return true;
}

const galleryItems = document.querySelectorAll('.gallery-item');
const lightbox = document.getElementById('lightbox');
const lightboxImage = document.querySelector('.lightbox-image');
const closeBtn = document.querySelector('.close');
const prevBtn = document.querySelector('.prev');
const nextBtn = document.querySelector('.next');

let currentIndex = 0;

// Open lightbox
galleryItems.forEach((item, index) => {
    item.addEventListener('click', () => {
        currentIndex = index;
        updateLightbox();
        lightbox.classList.remove('hidden');
    });
});

// Close lightbox
closeBtn.addEventListener('click', () => {
    lightbox.classList.add('hidden');
});

// Navigate to the previous image
prevBtn.addEventListener('click', () => {
    currentIndex = (currentIndex - 1 + galleryItems.length) % galleryItems.length;
    updateLightbox();
});

// Navigate to the next image
nextBtn.addEventListener('click', () => {
    currentIndex = (currentIndex + 1) % galleryItems.length;
    updateLightbox();
});

// Update lightbox with the current image
function updateLightbox() {
    const img = galleryItems[currentIndex].querySelector('img');
    lightboxImage.src = img.src;
    lightboxImage.alt = img.alt;
}

// Close lightbox on outside click
lightbox.addEventListener('click', (e) => {
    if (e.target === lightbox) {
        lightbox.classList.add('hidden');
    }
});
