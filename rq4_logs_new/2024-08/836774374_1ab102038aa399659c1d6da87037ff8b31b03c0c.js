
document.addEventListener('DOMContentLoaded', function() {
    const toggleButton = document.getElementById('toggleButton');
    const hiddenPlanets = document.querySelectorAll('.planethead[style*="visibility: hidden"]');
    const learnMoreButton = document.getElementById('button-learn');
    const sunDiv = document.getElementById('sun-div');
  

    // Toggle hidden planet names
    toggleButton.addEventListener('click', function() {
        hiddenPlanets.forEach(planet => {
            if (planet.style.visibility === 'hidden') {
                planet.style.visibility = 'visible';
            } else {
                planet.style.visibility = 'hidden';
            }
        });
    });

   
    learnMoreButton.addEventListener('click', function() {
        sunDiv.scrollIntoView({ behavior: 'smooth' });
    });
});