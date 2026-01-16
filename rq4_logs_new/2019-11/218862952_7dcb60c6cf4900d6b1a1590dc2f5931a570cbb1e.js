import auth from '../utilities/base';
export default function tryAutoLogin() {
  auth.auth().onAuthStateChanged(user => {
    if (user) {
      console.log(user.uid);
    } else {
      console.log('User is signed out');
    }
  });
}