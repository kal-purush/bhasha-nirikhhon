//declaration of input fields
var loginPro_Form = document.querySelector('#login_form');
var loginBtn = document.querySelector('#signIn');
var signed_nav_link = document.querySelectorAll('.signed_in');
var signedOut_nav_link = document.querySelectorAll('.signed_out');
var registerPro_Form = document.querySelector('#signUpPro');
var category_pros = document.querySelector('#category_pros');
var postForm = document.querySelector('#postForm');
var post_description = document.querySelector('#post_Description');
var post_verification_msg = document.querySelector('.post');
var form = document.querySelector('#new_post');
var email = document.querySelector('#register_email');
var password = document.querySelector('#password');
var typeOfUser = document.querySelector('#user_category');
var ratingsField = document.querySelector('#ratings-hidden');
var commentsForm = document.querySelector('.formComments');


//CONFIGURATED FIREBASE
var firebaseConfig = {
    apiKey: "AIzaSyBulcduYuKQfGIUbqq4xNbx-1MR65ia86s",
    authDomain: "pickapro-b9b63.firebaseapp.com",
    databaseURL: "https://pickapro-b9b63.firebaseio.com",
    projectId: "pickapro-b9b63",
    storageBucket: "pickapro-b9b63.appspot.com",
    messagingSenderId: "26515396537",
    appId: "1:26515396537:web:623bbaf83a4df9c0"
  };
  // Initialize Firebase
  
  var firebase = new firebase.initializeApp(firebaseConfig);

  var auth=firebase.auth();
  var db=firebase.firestore();

  