// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getAnalytics } from "firebase/analytics";
import { getDatabase, ref, set } from "firebase/database";
import { Bookmark } from './bookmark/type'
// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
// For Firebase JS SDK v7.20.0 and later, measurementId is optional
const firebaseConfig = {
  apiKey: "AIzaSyAP9LewVkw8TGZVDXZz9fyvKL8HkOki5KI",
  authDomain: "d-prowser.firebaseapp.com",
  projectId: "d-prowser",
  storageBucket: "d-prowser.appspot.com",
  messagingSenderId: "627346574140",
  appId: "1:627346574140:web:7249e68584307147cfac39",
  measurementId: "G-N30LQ2PYRT",
  databaseURL: "https://d-prowser-default-rtdb.firebaseio.com"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
const analytics = getAnalytics(app);

const database = getDatabase();

export const writeBookmarkData = (bookmark: Bookmark) => {
  const db = getDatabase(app);
  set(ref(db, 'Bookmark/' + bookmark.id), {
    id: bookmark.id,
    title: bookmark.title,
    url: bookmark.url,
    icon: bookmark.icon
  });
}