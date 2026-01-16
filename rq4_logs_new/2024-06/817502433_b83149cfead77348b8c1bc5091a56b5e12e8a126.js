document.getElementById('contact-form').addEventListener('submit', function(event) {
    event.preventDefault();

    let name = document.getElementById('name').value;
    let email = document.getElementById('email').value;
    let message = document.getElementById('message').value;

    // Simulate sending an email or notification to the company
    console.log(`Order placed by ${name} (${email}): ${message}`);

    alert('Thank you for your message! We will get back to you soon.');
    
    // Clear the form
    document.getElementById('contact-form').reset();
});
// Firebase configuration
const firebaseConfig = {
    apiKey: "YOUR_API_KEY",
    authDomain: "YOUR_AUTH_DOMAIN",
    projectId: "YOUR_PROJECT_ID",
    storageBucket: "YOUR_STORAGE_BUCKET",
    messagingSenderId: "YOUR_MESSAGING_SENDER_ID",
    appId: "YOUR_APP_ID"
};

// Initialize Firebase
const app = firebase.initializeApp(firebaseConfig);
const db = firebase.firestore();

document.getElementById('contact-form').addEventListener('submit', function(event) {
    event.preventDefault();

    let name = document.getElementById('name').value;
    let email = document.getElementById('email').value;
    let message = document.getElementById('message').value;

    // Add a new order to the Firestore database
    db.collection("orders").add({
        name: name,
        email: email,
        message: message,
        timestamp: firebase.firestore.FieldValue.serverTimestamp()
    })
    .then((docRef) => {
        console.log("Order added with ID: ", docRef.id);
        alert('Thank you for your message! We will get back to you soon.');
        document.getElementById('contact-form').reset();
    })
    .catch((error) => {
        console.error("Error adding order: ", error);
        alert('There was an error submitting your message. Please try again later.');
    });
});