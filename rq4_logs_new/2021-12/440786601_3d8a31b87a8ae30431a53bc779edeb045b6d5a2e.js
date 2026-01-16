// import firebase from "firebase/app";
// import "firebase/storage";
import { initializeApp } from "firebase/app";
import { getStorage } from "firebase/storage";

const firebaseConfig = {
  apiKey: "AIzaSyDrfKHAC8PLxCoqI1QaYcqdeiHwAfVgX5Y",
  authDomain: "certificate-2cf66.firebaseapp.com",
  projectId: "certificate-2cf66",
  storageBucket: "certificate-2cf66.appspot.com",
  messagingSenderId: "859614653778",
  appId: "1:859614653778:web:6c080864bf2f24f6e01e8b"
};
// const app = initializeApp(firebaseConfig);

// firebase.initializeApp(firebaseConfig);

// const storage = firebase.storage();

// export { storage, firebase as default };
const app = initializeApp(firebaseConfig);

const storage = getStorage(app);

export { storage, app };