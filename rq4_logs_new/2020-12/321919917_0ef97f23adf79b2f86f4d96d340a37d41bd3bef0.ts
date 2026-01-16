import firebase from 'firebase/app'
import 'firebase/firestore'
import { RoomInfoType } from '../interface/interface'

const firebaseConfig = {
  apiKey: process.env.REACT_APP_FB_API_KEY,
  // authDomain: process.env.REACT_APP_FB_AUTH_DOMAIN,
  databaseURL: process.env.REACT_APP_FB_DATABASE_URL,
  projectId: process.env.REACT_APP_FB_PROJECT_ID,
  // storageBucket: process.env.REACT_APP_FB_STORAGE_BUCKET,
  // messagingSenderId: process.env.REACT_APP_FB_MESSAGING_SENDER_ID,
  appId: process.env.REACT_APP_FB_APP_ID,
  // measurementId: process.env.REACT_APP_FB_MEASUREMENT_ID,
}
if (firebase.apps.length === 0) {
  firebase.initializeApp(firebaseConfig)
}
export default firebase
export const firestore = firebase.firestore()
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const addRoomFB = async (info: RoomInfoType): Promise<any> => {
  const roomList = firestore.collection('roomList')
  const ret = await roomList
    .add(info)
    .then((res) => res.id)
    .catch((error) => error)

  return ret
}