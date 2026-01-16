import { initializeApp } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-app.js";
import { getFirestore, collection, addDoc, getDocs } from "https://www.gstatic.com/firebasejs/10.8.0/firebase-firestore.js";

const firebaseConfig = {
  apiKey: "AIzaSyCJKYI1k7FF9TxV9DnJinNxP0W3SLhKayM",
  authDomain: "sn-data-ce6c2.firebaseapp.com",
  projectId: "sn-data-ce6c2",
  storageBucket: "sn-data-ce6c2.firebasestorage.app",
  messagingSenderId: "790874016811",
  appId: "1:790874016811:web:f7694c97302872686dee6e",
  measurementId: "G-CTNG164N8S"
};

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

// Function to add data
async function addData() {
    await addDoc(collection(db, "records"), {
        name: "John Doe",
        amount: 500,
        date: new Date()
    });
}

// Function to fetch data
async function fetchData() {
    const querySnapshot = await getDocs(collection(db, "records"));
    querySnapshot.forEach((doc) => {
        console.log(doc.id, " => ", doc.data());
    });
}

addData();
fetchData();