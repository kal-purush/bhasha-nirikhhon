import { initializeApp } from 'firebase/app'
import { getMessaging, getToken } from 'firebase/messaging'

const firebaseConfig = {
  apiKey: process.env.REACT_APP_FIREBASE_API_KEY,
  authDomain: process.env.REACT_APP_FIREBASE_AUTH_DOMAIN,
  projectId: process.env.REACT_APP_FIREBASE_PROJECT_ID,
  storageBucket: process.env.REACT_APP_FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.REACT_APP_FIREBASE_MESSAGING_SENDER_ID,
  appId: process.env.REACT_APP_FIREBASE_APP_ID,
  measurementId: process.env.REACT_APP_FIREBASE_MEASUREMENT_ID
}

const app = initializeApp(firebaseConfig)
const messaging = getMessaging(app)

export function requestPermission() {
  console.log('Requesting permission...')

  void Notification.requestPermission().then((permission) => {
    if (permission === 'granted') {
      console.log('Notification permission granted.')

      getToken(messaging, { vapidKey: process.env.REACT_APP_FIREBASE_VAPID_KEY })
        .then((token: string) => {
          if (token.length > 0) {
            console.log(`token: ${token}`)
          } else {
            // Show permission request UI
            console.log('No registration token available. Request permission to generate one.')
            // ...
          }
        })
        .catch((err) => {
          console.log('An error occurred while retrieving token. ', err)
          // ...
        })
    } else if (permission === 'denied') {
      console.log('Notification permission denied')
    }
  })
}