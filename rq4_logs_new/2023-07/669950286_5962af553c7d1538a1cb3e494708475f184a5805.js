// JavaScript code for header animation
window.addEventListener('DOMContentLoaded', () => {
    const headerText = document.getElementById('headerText');
  
    // Trigger the animation once the page has loaded
    headerText.style.animationPlayState = 'running';
  });


  document.addEventListener('DOMContentLoaded', function() {
    const navLinks = document.querySelectorAll('nav ul li a');
  
    navLinks.forEach(link => {
      link.addEventListener('click', function(event) {
        event.preventDefault();
        const targetId = link.getAttribute('href').substring(1);
        const targetSection = document.getElementById(targetId);
        if (targetSection) {
          const scrollOptions = {
            behavior: 'smooth',
            block: 'start'
          };
          targetSection.scrollIntoView(scrollOptions);
        }
      });
    });
  });

  document.addEventListener('DOMContentLoaded', function() {
    const nav = document.querySelector('nav');
    let isScrolling = false;
  
    window.addEventListener('scroll', function() {
      if (!isScrolling) {
        nav.classList.add('scrolling');
        isScrolling = true;
        setTimeout(function() {
          nav.classList.remove('scrolling');
          isScrolling = false;
        }, 1000); // Change the time (in milliseconds) to control the delay of hiding the navigation bar after scroll stops
      }
    });
  });
   

  window.onscroll = function() { toggleScrollUpButton() };

  function toggleScrollUpButton() {
    var scrollUpButton = document.getElementById("scrollUpButton");
    if (document.body.scrollTop > 20 || document.documentElement.scrollTop > 20) {
      scrollUpButton.style.display = "block";
    } else {
      scrollUpButton.style.display = "none";
    }
  }
  
  function scrollToTop() {
    document.body.scrollTop = 0;
    document.documentElement.scrollTop = 0;
  }
   
  