// DOM content loader
document.addEventListener('DOMContentLoaded', () => {
    // Function to rotate images in a carousel
    function autoRotateCarousel(carousel) {
        const images = carousel.querySelectorAll('img');
        let currentIndex = 0;
  
        // Function to show the next image
        function showNextImage() {
            images[currentIndex].style.opacity = 0; // Fade out current image
            setTimeout(() => {
                images[currentIndex].style.display = 'none';
                currentIndex = (currentIndex + 1) % images.length; // Move to the next index
                images[currentIndex].style.display = 'block'; // Show next image
                images[currentIndex].style.opacity = 1; // Fade in next image
            }, 500); // Wait for the fade out to finish
        }
  
        // Start the interval for automatic rotation (change every 3 seconds)
        setInterval(showNextImage, 3000);
    }
  
    // Initialize carousels
    const carousels = document.querySelectorAll('.image-carousel');
    carousels.forEach(autoRotateCarousel);

    // Swipe carousel for mobile devices
    carousels.forEach((carousel) => {
        let startX = 0;

        carousel.addEventListener('touchstart', (event) => {
            startX = event.touches[0].clientX;
        });

        carousel.addEventListener('touchmove', (event) => {
            const moveX = event.touches[0].clientX;
            const diffX = startX - moveX;

            if (Math.abs(diffX) > 50) {
                if (diffX > 0) {
                    showNextImage(); // Swipe left
                } else {
                    // Swipe right
                    images[currentIndex].style.opacity = 0; // Fade out current image
                    setTimeout(() => {
                        images[currentIndex].style.display = 'none';
                        currentIndex = (currentIndex - 1 + images.length) % images.length; // Previous index
                        images[currentIndex].style.display = 'block';
                        images[currentIndex].style.opacity = 1; // Fade in next image
                    }, 500); // Wait for the fade out to finish
                }
            }
        });
    });

    // Expanding the images for a fuller menu by using an array as an alternative method:
    // Uncomment this section to dynamically create images
    /*
    const images = [
        { src: 'brush.jpg', alt: 'Bruschetta', caption: 'Bruschetta - $8' },
        { src: 'soup.jpg', alt: 'Soup', caption: 'Soup - $6' },
        { src: 'cheese.jpg', alt: 'Cheese Plate', caption: 'Cheese Plate - $10' }
    ];

    images.forEach((imageData) => {
        const img = document.createElement('img');
        img.src = imageData.src;
        img.alt = imageData.alt;
        carousel.appendChild(img);

        const caption = document.createElement('div');
        caption.className = 'caption';
        caption.textContent = imageData.caption;
        carousel.appendChild(caption);
    });
    */
});