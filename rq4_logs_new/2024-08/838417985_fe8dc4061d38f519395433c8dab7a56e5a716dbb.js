document.addEventListener('DOMContentLoaded', () => {
  tailwind.config = {
    theme: {
      extend: {
        fontFamily: {
          montserrat: ['Montserrat', 'sans-serif'],
        },
      },
    }
  }

  const inputValue = document.getElementById('inputValue');
  const generateBtn = document.getElementById('generateBtn');
  const qrImg = document.getElementById('qrImg');
  const errorMsg = document.getElementById('errorMsg');


  window.addEventListener('reload', () => {
    inputValue.value = ''; // Clear the input field when the window reloads
  });


  generateBtn.addEventListener('click', (event) => {
    event.preventDefault();
    checkInput();
  });

  // Function to check input and generate QR code
  const checkInput = () => {
    const trimmedInput = inputValue.value.trim();
    console.log(trimmedInput);
    if (trimmedInput.length > 0) {
      // Set QR code image source
      qrImg.src = `https://api.qrserver.com/v1/create-qr-code/?size=150x150&data=${encodeURIComponent(trimmedInput)}`;
      errorMsg.textContent = '';
      inputValue.value = '';
    } else {
      // Clear QR code image source and show error message
      qrImg.src = '';
      errorMsg.textContent = 'Type Text Or Use URL To Generate';
    }
  };
})