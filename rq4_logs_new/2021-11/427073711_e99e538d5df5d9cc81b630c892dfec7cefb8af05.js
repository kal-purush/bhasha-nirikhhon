var firebaseConfig = {
    apiKey: "AIzaSyA-uHwYxs-xUCHYL1sNjPFfy8oBiWJyVdE",
    authDomain: "light-web.firebaseapp.com",
    databaseURL: "https://light-web-default-rtdb.firebaseio.com",
    projectId: "light-web",
    storageBucket: "light-web.appspot.com",
    messagingSenderId: "980096008717",
    appId: "1:980096008717:web:e460c8b82f3ec0fececb44",
    measurementId: "G-W84S124FCK"
};
firebase.initializeApp(firebaseConfig);
var database = firebase.database();

function myFunction() {
    var checkBox = document.getElementById("LED1");
    if (checkBox.checked == true) {
        console.log("ON")
        database.ref('LED').set(1);
    } else {
        console.log("OFF")
        database.ref('LED').set(0);
    }
}