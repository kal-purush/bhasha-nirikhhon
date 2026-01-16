  import firebase from 'firebase/app'
  import firestore from 'firebase/firestore'
  
  // Initialize Firebase
  var config = {
    apiKey: "AIzaSyDVaRM9rISFqrmdwm6hECdzecGkQ3HxeqI",
    authDomain: "vue-chat-560b2.firebaseapp.com",
    databaseURL: "https://vue-chat-560b2.firebaseio.com",
    projectId: "vue-chat-560b2",
    storageBucket: "vue-chat-560b2.appspot.com",
    messagingSenderId: "424330027142"
  };
  const firebaseApp = firebase.initializeApp(config);

  export default firebaseApp.firestore();