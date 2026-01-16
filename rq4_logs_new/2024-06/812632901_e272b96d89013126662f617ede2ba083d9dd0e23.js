document.addEventListener("DOMContentLoaded", (event) => {
  const form = document.getElementById('text-area-form');
  

  const textarea = document.querySelector('.new-tweet form textarea');
  const counter = document.querySelector('.new-tweet .counter');



  textarea.addEventListener('input', () => {
    const maxChars = 140;
    const currentChars = textarea.value.length;
    const remainingChars = maxChars - currentChars;
    
    counter.textContent = remainingChars;

    if (remainingChars < 0) {
      counter.classList.add('exceeded-limit');      
    } else {
      counter.classList.remove('exceeded-limit');
    }

    textarea.style.height = 'auto';
    textarea.style.height = textarea.scrollHeight + 'px';    
  });

  textarea.dispatchEvent(new Event('input'));
})