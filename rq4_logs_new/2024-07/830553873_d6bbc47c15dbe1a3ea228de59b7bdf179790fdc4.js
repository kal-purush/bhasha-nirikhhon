document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('email-form').addEventListener('submit', function(event) {
        event.preventDefault(); // Prevent the default form submission
        
        const checkEmail = () => {
            const email = document.querySelector('input[type="email"]');
            const emailValue = email.value;
            const errorElement = document.getElementById('error-message');

            const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
            
            if (emailValue === '') {
               errorElement.textContent = 'Please provide an email address';
            } else if (!emailPattern.test(emailValue)) {
                errorElement.textContent = 'Oops! Please check your email';
            } else {
                email.value = '';
                errorElement.textContent = '';
                // Optionally, handle successful email validation here
                console.log('Email submitted:', emailValue);
            }
        }
        
        checkEmail();
        
    });
    









});