import { initializeApp } from 'firebase/app';
import { getAuth } from 'firebase/auth';
import { getFirestore } from 'firebase/firestore';
import { ref } from 'vue';
import { onAuthStateChanged } from 'firebase/auth';
import { collection, doc, setDoc, getDoc } from 'firebase/firestore';

const firebaseConfig = {
    apiKey: "AIzaSyACHfAYEzjUFhSqT7QJV1EAW1Jx7x2Jqwc",
    authDomain: "cis371-semester-project-b860c.firebaseapp.com",
    projectId: "cis371-semester-project-b860c",
    storageBucket: "cis371-semester-project-b860c.appspot.com",
    messagingSenderId: "685988760069",
    appId: "1:685988760069:web:b1b374cdee9086dc5cea6c"
};

initializeApp(firebaseConfig);
const auth = getAuth();
const db = getFirestore();

const cart = ref([]);

async function getUserCart(userId) {
    const cartRef = doc(db, 'carts', userId);
    const cartDoc = await getDoc(cartRef);
  
    if (cartDoc.exists()) {
      cart.value = cartDoc.data().items;
    } else {
      cart.value = [];
    }
  
    return cart;
  }
  
  async function saveUserCart(userId, items) {
    const cartRef = doc(db, 'carts', userId);
    await setDoc(cartRef, { items });
  }
  
  onAuthStateChanged(auth, (user) => {
    if (user) {
      getUserCart(user.uid);
    } else {
      cart.value = [];
    }
  });
  
  export { auth, db, cart, getUserCart, saveUserCart };