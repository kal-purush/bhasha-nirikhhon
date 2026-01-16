document.addEventListener('DOMContentLoaded', () => {
    fetch('/components/navbar.html')
      .then(response => response.text())
      .then(data => {
        document.getElementById('navbar-container').innerHTML = data;
  
        // Initialize hamburger functionality here
        const hamburger = document.querySelector('.hamburger');
        const menu = document.querySelector('.menu');
  
        if (hamburger && menu) {
          hamburger.addEventListener('click', () => {
            menu.classList.toggle('active');
          });
        } else {
          console.error('Hamburger or menu not found');
        }
      })
      .catch(error => console.error('Error loading navbar:', error));
  });