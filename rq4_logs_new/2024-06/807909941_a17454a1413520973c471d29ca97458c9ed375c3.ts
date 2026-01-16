import { initializeApp, FirebaseApp } from "firebase/app";
import { getFunctions, connectFunctionsEmulator } from "firebase/functions";
import { connectAuthEmulator, getAuth, createUserWithEmailAndPassword, Auth } from "firebase/auth";
import { connectFirestoreEmulator, doc, getDoc, Firestore, getFirestore } from "firebase/firestore";

const APP_CONFIG = {
  "projectId": "potion-bce28",
  "appId": "1:91139147310:web:407b590998cdd83dbf01bf",
  "storageBucket": "potion-bce28.appspot.com",
  "apiKey": "AIzaSyAWqj_zYk5aKnhcAG2n_eoL_7dpJZLnxQY",
  "authDomain": "potion-bce28.firebaseapp.com",
  "messagingSenderId": "91139147310",
  "measurementId": "G-X92K3JKDK1"
};

describe("Firebase Functions", () => {
  let app: FirebaseApp
  let functions;
  let auth: Auth;
  let firestore: Firestore;

  beforeAll(() => {
    const appName = `test-app-${Date.now()}`;
    app = initializeApp(APP_CONFIG, appName);
  });

  beforeEach(() => {
    auth = getAuth(app);
    firestore = getFirestore(app);
    functions = getFunctions(app);

    connectAuthEmulator(auth, "http://localhost:9099", { disableWarnings: true });
    connectFirestoreEmulator(firestore, "localhost", 8080);
    connectFunctionsEmulator(functions, "127.0.0.1", 5001);
  });

  describe("createUserDoc", () => {
    it("creates a user doc when a new user is created", async () => {
      // Create a new user
      const email = `user-${Date.now()}@test.com`;
      const password = "password";
      const userCredential = await createUserWithEmailAndPassword(auth, email, password);
      expect(userCredential.user).toBeTruthy();

      // check if the user doc was created
      const docRef = doc(firestore, "users", userCredential.user.uid);
      const snap = await getDoc(docRef);
      expect(snap.exists()).toBe(true);
    }, 20000);
  });
});