import firebase from '../utils/firebase';
import * as Models from '../models/userModels';
import { async } from 'q';



function setUserInfo(fuser: firebase.User | null): Models.LoginUser | null {
  if (!fuser) {
      return null;
  }
  return {
    userId: fuser.uid,
    email: fuser.email ? fuser.email : '',
    displayName: fuser.displayName ? fuser.displayName : '',
    phoneNumber: fuser.phoneNumber ? fuser.phoneNumber : '',
    userName: fuser.displayName ? fuser.displayName : '',
  }
}

export const getUserInfromation = async (uid: string) => {
  try {
    let userInfromation = null;
    await firebase
    .firestore()
    .collection('users')
    .doc(uid)
    .get()
    .then(doc => {
      if(!doc.exists) {
        return;
      }
      const data = Object.assign({}, doc.data());
      userInfromation = data;
    }).catch(error => {
      throw new Error(error.message);
    });
    return { userInfromation };
  } catch(error) {
    return { error }
  };
};

export const userLogin = async () => {
  const provider = new firebase.auth.GoogleAuthProvider();
  try {
    let loginUser = null;
    await firebase
    .auth()
    .signInWithPopup(provider)
    .then((user) => {
      if(!user) {
        return;
      }
      const data = Object.assign({}, setUserInfo(user.user));
      loginUser = data;
    }).catch(error => {
      throw new Error(error.message);
    })
    return { loginUser };
  } catch(error) {
    return { error };
  };
};

export const userLogout = async () => {
  try {
    await firebase
    .auth()
    .signOut()
    .then(info => {
      console.log('user Logout API', info)
    }).catch(error => {
      throw new Error(error.message);
    })
    const success = { success: 'Logout User OK'}
    return { success };
  } catch(error) {
    return { error };
  };
};