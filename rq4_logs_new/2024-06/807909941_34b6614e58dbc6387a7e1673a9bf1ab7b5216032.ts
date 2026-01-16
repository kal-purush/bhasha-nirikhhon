import { TestBed } from '@angular/core/testing';
import { FirebaseApp, getApp, initializeApp, provideFirebaseApp } from '@angular/fire/app';
import { Firestore, collection, connectFirestoreEmulator, deleteDoc, disableNetwork, getDocs, getFirestore, provideFirestore } from '@angular/fire/firestore';
import { getAuth, provideAuth, connectAuthEmulator, Auth, createUserWithEmailAndPassword, signInWithEmailAndPassword, User, deleteUser, signOut } from '@angular/fire/auth';
import { environment } from '../../environments/environment';
import { DailyService } from './daily.service';

describe('DailyService', () => {
  let app: FirebaseApp;
  let firestore: Firestore;
  let auth: Auth
  let providedFirestore: Firestore;
  let appName: string;
  let currentUser: User|null;

  beforeEach(() => {
    appName = `firestore-tests-${Date.now()}`;
    TestBed.configureTestingModule({
      providers: [
        DailyService,
        provideFirebaseApp(() => initializeApp(environment.firebase.config, appName)),
        provideFirestore(() => {
          providedFirestore = getFirestore(getApp(appName));
          connectFirestoreEmulator(providedFirestore, 'localhost', 8080);
          return providedFirestore;
        }),
        provideAuth(() => {
          const auth = getAuth(getApp(appName));
          if (environment.firebase.emulators.auth) {
            connectAuthEmulator(auth, 'http://localhost:9099', { disableWarnings: true });
          }
          return auth;
        }),
      ],
    });
    app = TestBed.inject(FirebaseApp);
    firestore = TestBed.inject(Firestore);
    auth = TestBed.inject(Auth);
    currentUser = null;
  });

  afterEach(async () => {
    if (currentUser != null) {
      await deleteUser(currentUser);
    }
    await disableNetwork(firestore);
  });

  it('creates Daily Service instance', () => {
    const service = TestBed.inject(DailyService);
    expect(service).toBeTruthy();
  });

  it('saves daily', async () => {
    const email = `u-${Date.now()}@test.com`;
    const password = 'password';
    const userCredential = await createUserWithEmailAndPassword(auth, email, password);
    currentUser = userCredential.user;

    const service = TestBed.inject(DailyService);
    const result = await service.save('text', 'title');
    expect(result).toBe(true);
  });

  it('fails to save daily when not authorized', async () => {
    await signOut(auth);

    const service = TestBed.inject(DailyService);
    const result = await service.save('text', 'title');
    expect(result).toBe(false);
  });
});