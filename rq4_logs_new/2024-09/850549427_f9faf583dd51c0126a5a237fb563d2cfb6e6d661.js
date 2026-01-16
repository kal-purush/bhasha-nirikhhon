// Array to store contact form details
const formDataArray = [];

// Function to handle form submission
document.getElementById('contactForm').addEventListener('submit', function(event) {
    event.preventDefault(); // Prevent the form from submitting the traditional way

    // Get form values
    const name = document.getElementById('name').value;
    const email = document.getElementById('email').value;
    const message = document.getElementById('message').value;

    // Create an object to store the form data
    const formData = {
        name: name,
        email: email,
        message: message
    };

    // Push the form data object into the array
    formDataArray.push(formData);

    // Log the formDataArray to the console (for testing)
    console.log('Form Data Array:', formDataArray);

    // Optionally, clear the form fields after submission
    document.getElementById('contactForm').reset();

    // Alert the user that the form was submitted successfully
    alert('Thank you for contacting us! Your message has been saved.');
console.log(formDataArray);
});