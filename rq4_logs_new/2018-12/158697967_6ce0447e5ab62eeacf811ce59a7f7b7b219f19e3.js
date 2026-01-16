// Only import the compile function from handlebars instead of the entire library
import { compile } from 'handlebars';
import update from '../helpers/update';
import { getInstance } from '../firebase/firebase';

const firebase = getInstance();

// Import the template to use
const loginTemplate = require('../templates/login.handlebars');

// Log in
const login = (email, password) => {
  console.log(email);
  firebase.auth().signInWithEmailAndPassword(email, password)
    .then(() => window.location.replace('Home'))
    .catch(error => console.log(error));
};

// Function
export default () => {
  update(compile(loginTemplate)());
  const btnLogin = document.getItemById('login');

  // button clicked
  btnLogin.addEventListener('click', (e) => {
    e.preventDefault();
    const email = document.getElementById('email').value;
    const password = document.getElementById('password').value;
    login(email, password);
  });
};