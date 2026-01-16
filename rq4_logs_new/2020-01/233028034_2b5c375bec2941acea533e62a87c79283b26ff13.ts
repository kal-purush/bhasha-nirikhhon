import { getEnvName, getFirebase } from '../firebase';

export async function addUserToNetwork(uid: string) {
  const firebase = await getFirebase();
  const addUserToNetwork = firebase
    .functions('asia-northeast1')
    .httpsCallable('addUserToNetwork');
  await addUserToNetwork({
    uid,
    env: getEnvName()
  });
}