(async ()=>{

    const  AUTH_TOKEN = "<your user app token>";
    const BASE_URL = "<base url + endpoint>"
    MsgElem = document.getElementById("msg");
    TokenElem = document.getElementById("token");
    NotisElem = document.getElementById("notis");
    ErrElem = document.getElementById("err");

    // TODO: Replace firebaseConfig you get from Firebase Console
    var firebaseConfig = {
        apiKey: "",
        authDomain: "",
        projectId: "",
        storageBucket: "",
        messagingSenderId: "",
        appId: "",
        measurementId: "",
    };
    firebase.initializeApp(firebaseConfig);

    const messaging = firebase.messaging();
    const notificationToken = window.localStorage.getItem("notificationToken");
    if(notificationToken) console.log('true')
    if (notificationToken === null) {
        await messaging
            .requestPermission()
            .then(function () {
                MsgElem.innerHTML = "Notification permission granted.";
                console.log("Notification permission granted.");

                // get the token in the form of promise
                return messaging.getToken();
            })
            .then(async function (token) {
                TokenElem.innerHTML = "Device token is : <br>" + token;

                /**
                 *  Subscribe By Sending Token
                 */
                await fetch(
                    BASE_URL,
                    {
                        method: "PUT",
                        headers: {
                            "Content-Type": "application/json",
                            Authorization: "Bearer " + AUTH_TOKEN,
                        },
                        body: JSON.stringify({
                            token: token,
                        }),
                    }
                )
                    .then((response) => response)
                    .then((data) => {
                        console.log(data);
                        localStorage.setItem("notificationToken", token);
                    })
                    .catch((error) => console.error("Error:", error));
            })
            .catch(function (err) {
                ErrElem.innerHTML = ErrElem.innerHTML + "; " + err;
                console.log("Unable to get permission to notify.", err);
            });
    } else {
        console.log('already has token')
        console.log('notificationToken' ,notificationToken )
    }

    let enableForegroundNotification = true;
    messaging.onMessage(function (payload) {
        console.log("Message received. ", payload);
        NotisElem.innerHTML = NotisElem.innerHTML + JSON.stringify(payload);

        if (enableForegroundNotification) {
            let notification = payload.notification;
            navigator.serviceWorker.getRegistrations().then((registration) => {
                registration[0].showNotification(notification.title);
            });
        }
    });
})()