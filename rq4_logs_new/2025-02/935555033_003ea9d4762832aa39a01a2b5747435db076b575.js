const buttons = document.querySelectorAll('btn');
buttons.forEach(button => {
  button.addEventListener('click', () => {
    console.log('Button clicked');
  });
});