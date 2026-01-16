 // Dark mode toggle
    const toggleBtn = document.getElementById('darkToggle');
    toggleBtn.addEventListener('click', () => {
      document.body.classList.toggle('dark');
      toggleBtn.textContent = document.body.classList.contains('dark') ? '☀️' : '🌙';
    });

    // Typed text effect
    const typedTextSpan = document.getElementById('typed-text');
    const phrases = ["Pooja Bai", "Frontend Developer", "Full Stack Developer", "React Enthusiast"];
    let phraseIndex = 0;
    let letterIndex = 0;
    let typingDelay = 120;
    let erasingDelay = 80;
    let newPhraseDelay = 1800;
    let isDeleting = false;

    function type() {
      const currentPhrase = phrases[phraseIndex];
      if (!isDeleting && letterIndex <= currentPhrase.length) {
        typedTextSpan.textContent = currentPhrase.substring(0, letterIndex);
        letterIndex++;
        setTimeout(type, typingDelay);
      } else if (isDeleting && letterIndex >= 0) {
        typedTextSpan.textContent = currentPhrase.substring(0, letterIndex);
        letterIndex--;
        setTimeout(type, erasingDelay);
      } else {
        isDeleting = !isDeleting;
        if (!isDeleting) {
          phraseIndex = (phraseIndex + 1) % phrases.length;
        }
        setTimeout(type, newPhraseDelay);
      }
    }
    document.addEventListener("DOMContentLoaded", () => {
      setTimeout(type, 500);
    });

    // Generate random pastel particles inside .particles
    const particlesContainer = document.querySelector('.particles');
    const numberOfParticles = 25;

    function randomBetween(min, max) {
      return Math.random() * (max - min) + min;
    }

    for(let i=0; i<numberOfParticles; i++) {
      const particle = document.createElement('span');
      particle.style.left = `${randomBetween(0, 100)}%`;
      particle.style.top = `${randomBetween(0, 100)}%`;
      const size = randomBetween(4, 10);
      particle.style.width = `${size}px`;
      particle.style.height = `${size}px`;
      particle.style.animationDelay = `${randomBetween(0, 10)}s`;
      particle.style.opacity = randomBetween(0.15, 0.35);
      particlesContainer.appendChild(particle);
    }


  window.addEventListener("scroll", () => {
    document.querySelectorAll(".timeline-item").forEach(item => {
      if (item.getBoundingClientRect().top < window.innerHeight - 100) {
        item.classList.add("show");
      }
    });
  });

  function isInViewport(element) {
  const rect = element.getBoundingClientRect();
  return (
    rect.top >= 0 &&
    rect.bottom <= (window.innerHeight || document.documentElement.clientHeight)
  );
}

function animateSkills() {
  const progressBars = document.querySelectorAll('.progress');
  progressBars.forEach(bar => {
    if (isInViewport(bar.parentElement)) {
      bar.style.width = bar.getAttribute('data-width');
    } else {
      bar.style.width = '0';
    }
  });
}

window.addEventListener('scroll', animateSkills);
window.addEventListener('load', animateSkills);

const seeMoreBtn = document.getElementById('seeMoreBtn');
const moreProjects = document.getElementById('moreProjects');

seeMoreBtn.addEventListener('click', () => {
  if(moreProjects.classList.contains('show')) {
    moreProjects.classList.remove('show');
    seeMoreBtn.textContent = "See More Projects";
  } else {
    moreProjects.classList.add('show');
    seeMoreBtn.textContent = "See Less";
  }
});




  form.addEventListener("submit", function (e) {
    e.preventDefault(); // Stop default form submission
    popup.style.display = "flex"; // Show popup
    form.reset(); // Reset form values
  });

  function closePopup() {
    popup.style.display = "none";
  }



  
  const form = document.getElementById("contactForm");
  const popup = document.getElementById("successPopup");

  form.addEventListener("submit", async function (e) {
    e.preventDefault();

    const formData = {
      name: form.name.value,
      email: form.email.value,
      message: form.message.value,
    };

    try {
      const response = await fetch("http://127.0.0.1:5000/submit_contact", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData),
      });

      if (response.ok) {
        popup.style.display = "flex";
        form.reset();
      } else {
        alert("Something went wrong. Try again.");
      }
    } catch (err) {
      console.error("Error:", err);
      alert("Error submitting the form.");
    }
  });

  function closePopup() {
    popup.style.display = "none";
  }

  document.getElementById("contact-form").addEventListener("submit", function (e) {
  e.preventDefault(); // prevent form from reloading page

  const name = document.getElementById("name").value;
  const email = document.getElementById("email").value;
  const message = document.getElementById("message").value;

  fetch("/submit_contact", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ name, email, message })
  })
  .then(res => res.json())
  .then(data => {
    alert("Message sent successfully!");
  })
  .catch(err => {
    alert("Failed to send message.");
    console.error(err);
  });
});
