import firebase from 'firebase/app'
import 'firebase/messaging'

const firebaseConfig = {
  apiKey: process.env.REACT_APP_FIREBASE_API_KEY,
  authDomain: process.env.REACT_APP_FIREBASE_AUTH_DOMAIN,
  projectId: process.env.REACT_APP_FIREBASE_PROJECT_ID,
  storageBucket: process.env.REACT_APP_FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.REACT_APP_FIREBASE_MESSAGING_SENDER_ID,
  appId: process.env.REACT_APP_FIREBASE_APP_ID,
  measurementId: process.env.REACT_APP_FIREBASE_MEASUREMENT_ID
}

firebase.initializeApp(firebaseConfig)

const messaging = firebase.messaging()

export function requestPermission() {
  void Notification.requestPermission().then((permission) => {
    if (permission === 'granted') {
      messaging
        .getToken({ vapidKey: process.env.REACT_APP_FIREBASE_VAPID_KEY })
        .then((token: string) => {
          if (token.length > 0) {
            console.log('푸시 토큰 받아옴')
            console.log(`---> ${token}`)
          } else {
            console.log('푸시 토큰이 유효하지 않음')
          }
        })
        .catch((err) => {
          console.log('푸시 토큰 가져오는 중 에러남')
          console.log(`---> `, err)
        })
    } else if (permission === 'denied') {
      console.log('푸시 DENIED')
    }
  })
}