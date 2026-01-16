document.addEventListener('DOMContentLoaded', () => {
    fetch('data.json')
        .then(response => response.json())
        .then(data => {
            const plantCardsContainer = document.getElementById('plant-cards');
            data.forEach(plant => {
                const card = document.createElement('div');
                card.className = 'card';

                const images = plant.images.map((image, index) => `
                    <img src="${image}" alt="${plant['common-name']}" style="${index === 0 ? '' : 'display: none;'}" data-fullsrc="${image}">
                `).join('');

                card.innerHTML = `
                    <div class="carousel-container">
                        <div class="carousel-images">
                            ${images}
                        </div>
                        <div class="button-container">
                            <button class="carousel-button left">&lt;</button>
                            <button class="carousel-button right">&gt;</button>
                        </div>
                    </div>
                    <h2>${plant['scientific-name']}</h2> <!-- Make scientific name the primary, bold name -->
                    <p><strong>Common Name:</strong> ${plant['common-name']}</p> <!-- Display the common name normally -->
                    <p><strong>Family:</strong> ${plant['family-name']}</p>
                    <p><strong>Growth Form:</strong> ${plant['growth-form']}</p>
                    <p><strong>Conservation Status:</strong> ${plant['conservation-status']}</p>
                `;
                plantCardsContainer.appendChild(card);

                const leftButton = card.querySelector('.carousel-button.left');
                const rightButton = card.querySelector('.carousel-button.right');
                const imagesContainer = card.querySelector('.carousel-images');
                const imagesElements = card.querySelectorAll('.carousel-images img');
                let currentIndex = 0;

                leftButton.addEventListener('click', () => {
                    imagesElements[currentIndex].style.display = 'none';
                    currentIndex = (currentIndex - 1 + imagesElements.length) % imagesElements.length;
                    imagesElements[currentIndex].style.display = 'block';
                });

                rightButton.addEventListener('click', () => {
                    imagesElements[currentIndex].style.display = 'none';
                    currentIndex = (currentIndex + 1) % imagesElements.length;
                    imagesElements[currentIndex].style.display = 'block';
                });

                imagesElements.forEach(img => {
                    img.addEventListener('dblclick', () => {
                        openModal(img.getAttribute('data-fullsrc'));
                    });
                });
            });

            // Horizontal scroll with mouse wheel
            plantCardsContainer.addEventListener('wheel', (event) => {
                event.preventDefault();
                plantCardsContainer.scrollLeft += event.deltaY;
            });

            // Horizontal scroll with mouse drag
            let isDown = false;
            let startX, scrollLeft;

            plantCardsContainer.addEventListener('mousedown', (e) => {
                isDown = true;
                plantCardsContainer.classList.add('active');
                startX = e.pageX - plantCardsContainer.offsetLeft;
                scrollLeft = plantCardsContainer.scrollLeft;
            });

            plantCardsContainer.addEventListener('mouseleave', () => {
                isDown = false;
                plantCardsContainer.classList.remove('active');
            });

            plantCardsContainer.addEventListener('mouseup', () => {
                isDown = false;
                plantCardsContainer.classList.remove('active');
            });

            plantCardsContainer.addEventListener('mousemove', (e) => {
                if (!isDown) return;
                e.preventDefault();
                const x = e.pageX - plantCardsContainer.offsetLeft;
                const walk = (x - startX) * 3; // Adjust scroll speed
                plantCardsContainer.scrollLeft = scrollLeft - walk;
            });

            // Horizontal scroll with touch devices
            let startXTouch, scrollLeftTouch;

            plantCardsContainer.addEventListener('touchstart', (event) => {
                startXTouch = event.touches[0].pageX;
                scrollLeftTouch = plantCardsContainer.scrollLeft;
            });

            plantCardsContainer.addEventListener('touchmove', (event) => {
                const x = event.touches[0].pageX;
                const walk = (startXTouch - x) * 2; // Adjust scroll speed
                plantCardsContainer.scrollLeft = scrollLeftTouch + walk;
            });
        })
        .catch(error => console.error('Error fetching data:', error));

    function openModal(imageSrc) {
        const modal = document.createElement('div');
        modal.className = 'modal';
        modal.innerHTML = `
            <div class="modal-content">
                <span class="close">&times;</span>
                <img src="${imageSrc}">
            </div>
        `;
        document.body.appendChild(modal);
        modal.style.display = 'block';

        const closeButton = modal.querySelector('.close');
        closeButton.addEventListener('click', () => {
            modal.style.display = 'none';
            document.body.removeChild(modal);
        });

        modal.addEventListener('click', (event) => {
            if (event.target === modal) {
                modal.style.display = 'none';
                document.body.removeChild(modal);
            }
        });
    }

    // Prevent right-click
    document.addEventListener('contextmenu', (event) => {
        event.preventDefault();
    });
});