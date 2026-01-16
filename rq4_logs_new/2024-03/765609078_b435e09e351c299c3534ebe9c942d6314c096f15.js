// DOM Ready
document.addEventListener("DOMContentLoaded", function () {
    // Add event listeners or other setup functions here
    init();
});

// Initialization function
function init() {
    // Add initialization tasks here
    // Example: addEventListeners();
}

// Example of adding event listeners
function addEventListeners() {
    // Example: document.getElementById("myButton").addEventListener("click", myButtonClickHandler);
}

// Example of handling form submission
function handleFormSubmission(event) {
    event.preventDefault();
    // Gather form data
    const formData = new FormData(event.target);
    const serializedFormData = Object.fromEntries(formData.entries());
    
    // Example: Send data to API endpoint via AJAX
    fetch('/api/orders', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + getAuthToken() // Get JWT token from localStorage
        },
        body: JSON.stringify(serializedFormData)
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return response.json();
    })
    .then(data => {
        // Handle successful response
        console.log(data);
    })
    .catch(error => {
        // Handle error
        console.error('There was a problem with the fetch operation:', error);
    });
}

// Example function to get JWT token from localStorage
function getAuthToken() {
    return localStorage.getItem('jwt_token');
}