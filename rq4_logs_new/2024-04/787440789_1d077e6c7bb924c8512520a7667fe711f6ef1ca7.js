
const elements = document.querySelectorAll('.typing-effect');


elements.forEach(element => {
  const text = element.innerText;
  let index = 0;
  element.innerText = '';

  
  const timer = setInterval(() => {
    element.innerText = text.substring(0, index);
    if (index < text.length) {
      element.innerText += '|'; 
      index++;
    } else {
      element.innerText = text; 
      index = 0; 
    }
  }, 100); 
});








const titleElement = document.getElementById('glowing-heading');

function startGlowing() {
  titleElement.classList.add('glowing-effect');
}

function stopGlowing() {
  titleElement.classList.remove('glowing-effect');
}

startGlowing(); 


const containerElement = document.querySelector('.container');
const imageElement = document.querySelector('.about img');

containerElement.addEventListener('mouseover', function() {
  imageElement.style.filter = 'brightness(200%)';
});

containerElement.addEventListener('mouseout', function() {
  imageElement.style.filter = '';
});



document.addEventListener('mouseover', function(event) {
  if (event.target.matches('#myElement')) {
    event.target.style.backgroundColor = 'red';
  }
});

document.addEventListener('mouseout', function(event) {
  if (event.target.matches('#myElement')) {
    event.target.style.backgroundColor = '';
  }
});




const theElement = document.getElementById('myElement');

theElement.addEventListener('mousemove', function(event) {
  const rect = theElement.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const centerX = rect.width / 2;
  const centerY = rect.height / 2;
  const moveX = (centerX - x) / centerX * 10;
  const moveY = (centerY - y) / centerY * 10;
  theElement.style.backgroundPosition = `${moveX}px ${moveY}px`;
});

