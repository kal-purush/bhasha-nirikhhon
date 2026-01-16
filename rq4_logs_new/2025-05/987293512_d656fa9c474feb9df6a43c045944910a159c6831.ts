import { initializeApp } from "firebase/app";
import { getAuth } from "firebase/auth";

const firebaseConfig = {
  apiKey: "AIzaSyDRyEj08BBD0RvSm6P0YV2rioRUPvk62F8",
  authDomain: "webcarros-87efa.firebaseapp.com",
  projectId: "webcarros-87efa",
  storageBucket: "webcarros-87efa.firebasestorage.app",
  messagingSenderId: "197909081618",
  appId: "1:197909081618:web:511496813274130a8632cb",
  measurementId: "G-GQTFN8FPSM",
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);

export { app, auth };