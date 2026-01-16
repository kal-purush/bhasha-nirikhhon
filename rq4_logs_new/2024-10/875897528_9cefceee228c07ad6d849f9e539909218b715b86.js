// Payment Page
function showPayment(option) {
    const paymentOptions = ['upi', 'netbanking', 'card'];
    paymentOptions.forEach(opt => {
        const fields = document.getElementById(`${opt}-fields`);
        const circleInner = document.querySelector(`.payment-option[onclick="showPayment('${opt}')"] .circle-inner`);
        const circle = document.querySelector(`.payment-option[onclick="showPayment('${opt}')"] .circle`);

        if (opt === option) {
            fields.style.display = 'block';
            circleInner.style.display = 'block';
            circle.classList.add('selected');
        } else {
            fields.style.display = 'none';
            circleInner.style.display = 'none';
            circle.classList.remove('selected');
        }
    });
}

function selectBank(bankName) {
    const dropdown = document.getElementById('bankDropdown');
    dropdown.value = bankName;
}

// Initialize default selection
document.addEventListener('DOMContentLoaded', () => {
    showPayment('upi');  // Show UPI option by default
});

// Payments Page Input
function formatCardNumber(input) {
    // Remove any non-digit character
    input.value = input.value.replace(/[^0-9]/g, '');
    
    // Add dashes after every 4 digits
    let formattedValue = input.value.replace(/(\d{4})(?=\d)/g, '$1-');
    
    // Limit to 29 characters (24 digits + 5 dashes)
    if (formattedValue.length > 29) {
        formattedValue = formattedValue.slice(0, 29);
    }
    
    input.value = formattedValue;
}
function formatExpiryDate(input) {
    // Remove any non-digit character
    input.value = input.value.replace(/[^0-9]/g, '');

    // Insert '/' after the first two digits
    if (input.value.length > 2) {
        input.value = input.value.slice(0, 2) + '/' + input.value.slice(2, 4);
    }

    // Limit to 5 characters (2 digits, a '/', 2 digits)
    if (input.value.length > 5) {
        input.value = input.value.slice(0, 5);
    }
}

// Payment Gateway Buttons
// Select all specified buttons by their classes and prevent their actions
document.querySelectorAll('.bhim-upi, .phone-pay, .paytm, .google-pay, .sbi, .kotak, .axis, .hdfc, .submit').forEach(button => {
    button.addEventListener('click', (event) => {
      // Prevent page reloads or any default button action
      event.preventDefault();
      event.stopPropagation();
  
      // Set the title message
      button.title = "Sorry, your request cannot be processed at this moment.";
  
      // Temporarily display the message for 4 seconds
      setTimeout(() => {
        button.removeAttribute('title');
      }, 4000);
    });
  });
  
  // Disable form submission to prevent reloads
  document.querySelectorAll('form').forEach(form => {
    form.addEventListener('submit', (event) => {
      // Prevent any form submission or page reload
      event.preventDefault();
    });
  });
  