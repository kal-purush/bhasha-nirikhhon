import { readFileSync } from 'fs';
import {
  initializeTestEnvironment,
  assertSucceeds,
  assertFails,
  RulesTestEnvironment,
} from '@firebase/rules-unit-testing';
import { addDoc, collection, setLogLevel } from 'firebase/firestore';


let testEnv: RulesTestEnvironment;

describe('Firestore rules', () => {
  setLogLevel('error');
  beforeAll(async () => {
    testEnv = await initializeTestEnvironment({
      projectId: 'my-test-project',
      firestore: {
        rules: readFileSync('./firestore/firestore.rules', 'utf8'),
        host: 'localhost',
        port: 8080,
      }
    });
  });

  beforeEach(async() => {
    await testEnv.clearFirestore();
  })

  it('sanity check', () => {
    expect(true).toBe(true);
  });

  it('reject write if unauthenticated', async () => {
    let unauthedDb = testEnv.unauthenticatedContext().firestore();
    const morningPagesRef = collection(unauthedDb, 'morningPages');
    const err = await assertFails(addDoc(morningPagesRef, { text: 'hello' }));
    expect(err.code).toBe('permission-denied' || 'PERMISSION_DENIED');
  });

  it('allow write if authenticated', async () => {
    let authedDb = testEnv.authenticatedContext('user').firestore();
    const morningPagesRef = collection(authedDb, 'morningPages');
    await assertSucceeds(addDoc(morningPagesRef, { text: 'hello' }));
  });
});