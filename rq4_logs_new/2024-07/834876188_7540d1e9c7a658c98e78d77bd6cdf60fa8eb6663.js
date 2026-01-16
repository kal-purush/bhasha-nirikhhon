function createHeart() {
    const heart = document.createElement('div');
    heart.classList.add('heart');
    heart.style.width = `${Math.floor(Math.random() * 65) + 10}px`;
    heart.style.height = heart.style.width;
    heart.style.left = `${Math.floor(Math.random() * 100) + 1}%`;
    heart.style.background = `rgba(255, ${Math.floor(Math.random() * 25) + 100 - 25}, ${Math.floor(Math.random() * 25) + 100}, 1)`;
    const duration = Math.floor(Math.random() * 5) + 5;
    heart.style.animation = `love ${duration}s ease`;
    return heart;
  }
  
  // Get the container element where the hearts will be added
  const container = document.querySelector('.bg_heart');
  
  // Define a function that removes hearts that have gone off screen
  function removeHearts() {
    const hearts = container.querySelectorAll('.heart');
    hearts.forEach((heart) => {
      const top = parseFloat(getComputedStyle(heart).getPropertyValue('top'));
      const width = parseFloat(getComputedStyle(heart).getPropertyValue('width'));
      if (top <= -100 || width >= 150) {
        heart.remove();
      }
    });
  }
  
  // Define a function that repeatedly adds hearts to the container
  function addHeart() {
    const heart1 = createHeart();
    const heart2 = createHeart();
    container.appendChild(heart1);
    container.appendChild(heart2);
    setTimeout(removeHearts, 1000);
  }
  
  // Start the animation loop
  const love = setInterval(addHeart, 500);