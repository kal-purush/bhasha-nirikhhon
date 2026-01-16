// Give the service worker access to Firebase Messaging.
// Note that you can only use Firebase Messaging here. Other Firebase libraries
// are not available in the service worker.importScripts('https://www.gstatic.com/firebasejs/7.23.0/firebase-app.js');
importScripts('https://www.gstatic.com/firebasejs/8.3.2/firebase-app.js');
importScripts('https://www.gstatic.com/firebasejs/8.3.2/firebase-messaging.js');
/*
Initialize the Firebase app in the service worker by passing in the messagingSenderId.
*/
firebase.initializeApp({
    apiKey: "AIzaSyDlXc7Zceehcn-2kM7AXloUCRpqDyRIzYY",
    authDomain: "push-notification-d9539.firebaseapp.com",
    projectId: "push-notification-d9539",
    storageBucket: "push-notification-d9539.appspot.com",
    messagingSenderId: "585225948031",
    appId: "1:585225948031:web:4730e7cc736d76c484284a",
    measurementId: "G-74RB5C2JCH"
});

// Retrieve an instance of Firebase Messaging so that it can handle background
// messages.
const messaging = firebase.messaging();
messaging.setBackgroundMessageHandler(function(payload) {
    console.log("Message received.", payload);
    const title = "Hello world is awesome";
    const options = {
        body: payload.notification.body,
        icon: "https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png",
        badge: "https://www.google.com/images/branding/googlelogo/2x/googlelogo_color_272x92dp.png",
        vibrate: [100, 50, 100],
        data: {
            dateOfArrival: Date.now(),
            primaryKey: 1,
        }
    };
    return self.registration.showNotification(
        title,
        options,
    );
});