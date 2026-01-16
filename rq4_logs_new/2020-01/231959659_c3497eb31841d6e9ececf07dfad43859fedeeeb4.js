import firebase from 'firebase/app'
import 'firebase/firestore'

const firestore = firebase.firestore();

firestore.collection('users').doc('yIjEaCheAaEObVPR6ZiQ').collection('cartItens').doc('CXGyIGsz5yQXglJZ2nVH')
// also
firestore.doc('/users/yIjEaCheAaEObVPR6ZiQ/cartItens/CXGyIGsz5yQXglJZ2nVH')

    