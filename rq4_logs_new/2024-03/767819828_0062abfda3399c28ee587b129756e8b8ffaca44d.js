function login() {
    // Assume there is a function to check login credentials, returning the username if successful
    const username = checkLoginCredentials();

    if (username) {
        // Display greeting message if login is successful
        const greetingMessage = document.getElementById('greetingMessage');
        greetingMessage.textContent = 'Hello ' + username;
        greetingMessage.style.display = 'block';
    } else {
        // Display error message and clear input fields if login is unsuccessful
        alert('Incorrect login credentials. Please try again.');
        document.getElementById('username').value = '';
        document.getElementById('password').value = '';
    }
}

function checkLoginCredentials() {
  
}