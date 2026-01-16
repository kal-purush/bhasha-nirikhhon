const cards = document.querySelectorAll('.card');
  const dots = document.querySelectorAll('.dot');
  let currentIndex = 0;

  function showCard(index) {
    dots.forEach(dot => dot.classList.remove('active-dot'));
    dots[index].classList.add('active-dot');
    currentIndex = index;
  }

  // Show initial cards and active dot
  showCard(currentIndex);

  // Loop through each card
  cards.forEach((card, index) => {
  // Select the video thumbnail within the card
  const videoThumbnail = card.querySelector('.video-thumbnail');
  // Add event listener to the video thumbnail
  videoThumbnail.addEventListener('click', function() {
    // Toggle the active class for the video within this card
    card.querySelector('.video').classList.toggle('active');
  });
});

    // Wait for the DOM content to be loaded
    document.addEventListener('DOMContentLoaded', function() {
        // Select the fourth-container element
        const fourthContainer = document.querySelector('.fourth-container');
        // Select the video thumbnail within the fourth-container
        const videoThumbnail = fourthContainer.querySelector('.live');
        // Add event listener to the video thumbnail
        videoThumbnail.addEventListener('click', function() {
        // Toggle the active class for the video within this fourth-container
        fourthContainer.querySelector('.livevideo').classList.toggle('active');
    });
  });