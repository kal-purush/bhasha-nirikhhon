import { initializeApp } from "firebase/app";
import { getFirestore, collection, getDocs, where, getDoc, doc, query, addDoc } from 'firebase/firestore'

const firebaseConfig = {
  apiKey: "AIzaSyCDAJ1J9yG7RrmZ6vZ43H8x6FoyWKZyObo",
  authDomain: "khajits-corner.firebaseapp.com",
  projectId: "khajits-corner",
  storageBucket: "khajits-corner.appspot.com",
  messagingSenderId: "147757451461",
  appId: "1:147757451461:web:f642456cc1f8ad333a08ac"
};

const appFirebase = initializeApp(firebaseConfig);
const db = getFirestore(appFirebase);

async function getData() {
    const productosRef = collection(db, "productos");
    const docSnap = await getDocs(productosRef);
    const documento = docSnap.docs;
    const docsData = documento.map((item) => {
        return {...item.data(), id: item.id};
    })
    return docsData;
}

async function getProduct(id) {
    const docRef = doc(db, "productos",id);
    const docSnap = await getDoc(docRef);

    if(docSnap.exists()) {
        return {...docSnap.data(), id: docSnap.id};
    } else {
        throw new Error("No se encontro el producto.");
    }
}

async function getCategory(categoryId) {
    const productosRef = collection(db, "productos");
    const queryResponse = query(productosRef, where("categoryId", "==", categoryId));
    const docSnap = await getDocs(queryResponse);
    const documentos = docSnap.docs;

    return documentos.map((item) => ({...item.data(), id: item.id}));
}


async function crearOrden(orderData) {
    const collectionRef = collection(db, "ordenes");
    const docCreado = await addDoc(collectionRef, orderData);

    return(docCreado.id);
}

async function getOrden(id) {
    const docRef = doc(db, "ordenes", id);
    const docSnap = await getDoc(docRef);

    return {...docSnap.data(), id: docSnap.id};
}

export {getData, getProduct, getCategory, crearOrden, getOrden};