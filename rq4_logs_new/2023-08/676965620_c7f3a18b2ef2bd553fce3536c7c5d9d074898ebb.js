document.addEventListener("DOMContentLoaded", function () {
    const video = document.getElementById("videoElement");
    const playPauseBtn = document.getElementById("playPauseBtn");
    
    playPauseBtn.addEventListener("click", function () {
      if (video.paused) {
        video.play();
        playPauseBtn.innerHTML = '<i class="fas fa-pause"></i>';
      } else {
        video.pause();
        playPauseBtn.innerHTML = '<i class="fas fa-play"></i>';
      }
    });
  });



  document.addEventListener("DOMContentLoaded", function () {
    const readMoreBtn = document.getElementById("readMoreBtn");
    const disclaimerText = document.querySelector(".disclaimer-text");

    readMoreBtn.addEventListener("click", function () {
        disclaimerText.style.maxHeight = disclaimerText.style.maxHeight ? "" : "none";
        readMoreBtn.textContent = disclaimerText.style.maxHeight ? "Read Less" : "Read More";
    });
    
});



const dashboardContainer = document.querySelector('.dashboard-container');
const dashboard = document.querySelector('.dashboard');
let isDragging = false;
let startPosition = 0;
let scrollRight = 0;

dashboardContainer.addEventListener('mousedown', (e) => {
    isDragging = true;
    startPosition = e.clientX - dashboardContainer.getBoundingClientRect().right;
    scrollRight = dashboard.scrollRight;
    dashboard.style.transition = 'none';
    e.preventDefault();
});

document.addEventListener('mousemove', (e) => {
    if (!isDragging) return;
    e.preventDefault();
    const x = e.clientX - dashboardContainer.getBoundingClientRect().right;
    const scroll = startPosition - x;
    dashboard.scrollRight = scrollRight - scroll;
});

document.addEventListener('mouseup', () => {
    if (!isDragging) return;
    isDragging = false;
    dashboard.style.transition = 'transform 0.3s ease';
    scrollToImage();
});

function scrollToImage() {
    const currentImage = Math.round(dashboard.scrollRight / dashboardContainer.offsetWidth);
    const targetImage = currentImage * dashboardContainer.offsetWidth;
    dashboard.style.transform = `translateX(-${targetImage}px)`;
}




document.addEventListener('DOMContentLoaded', function () {
  const dashboardContainer = document.querySelector('.dashboard-container');
  const dashboard = document.querySelector('.dashboard');
  const dots = document.querySelectorAll('.dot');
  let isDragging = false;
  let startPosition = 0;
  let scrollRight = 0;

  dashboardContainer.addEventListener('mousedown', (e) => {
    isDragging = true;
    startPosition = e.clientX - dashboardContainer.getBoundingClientRect().left + dashboard.scrollLeft;
    scrollRight = dashboard.scrollLeft;
    dashboard.style.transition = 'none';
    e.preventDefault();
  });

  document.addEventListener('mousemove', (e) => {
    if (!isDragging) return;
    e.preventDefault();
    const x = e.clientX - dashboardContainer.getBoundingClientRect().left;
    const scroll = x - startPosition;
    dashboard.scrollLeft = scrollRight + scroll;
  });

  document.addEventListener('mouseup', () => {
    if (!isDragging) return;
    isDragging = false;
    dashboard.style.transition = 'transform 0.3s ease';
  });

  document.addEventListener('keydown', (e) => {
    if (isDragging && (e.key === 'ArrowLeft' || e.key === 'ArrowRight')) {
      e.preventDefault();
    }
  });

  dashboard.addEventListener('scroll', updateDots);

  function updateDots() {
    const scrollPosition = dashboard.scrollLeft;
    const imageWidth = dashboardContainer.offsetWidth;

    dots.forEach((dot, index) => {
      const imageIndex = Math.floor((scrollPosition + imageWidth / 2) / imageWidth);
      if (imageIndex === index) {
        dot.style.backgroundColor = '#1abc9c'; // Active dot color
      } else {
        dot.style.backgroundColor = 'transparent'; // Inactive dot color
      }
    });
  }
});


document.addEventListener('DOMContentLoaded', function () {
  const accordionQuestions = document.querySelectorAll('.accordion-question');

  accordionQuestions.forEach(question => {
      question.addEventListener('click', function () {
          this.parentElement.classList.toggle('active');
      });
  });
});

