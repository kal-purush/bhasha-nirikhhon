document.getElementById('dark-mode-toggle').addEventListener('change', function() {
    document.body.classList.toggle('dark-mode', this.checked);
});
    

    window.onload = function () {
    const progressBars = document.querySelectorAll('.progress-bar');
  
    progressBars.forEach((bar) => {
      const targetWidth = bar.getAttribute('aria-valuenow'); 
      bar.style.setProperty('--target-width', `${targetWidth}%`); 
    });
  };