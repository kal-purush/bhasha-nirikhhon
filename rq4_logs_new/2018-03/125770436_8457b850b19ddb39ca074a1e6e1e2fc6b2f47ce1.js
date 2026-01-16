import Rebase from 're-base';
import firebase from 'firebase';

const firebaseApp = firebase.initializeApp({
	apiKey: 'AIzaSyAG3kgca8utSQqU1mfIKGoVGIGcgqLSrM4',
	authDomain: 'restaurant-menu-751ae.firebaseapp.com',
	databaseURL: 'https://restaurant-menu-751ae.firebaseio.com'
});

const base = Rebase.createClass(firebaseApp.database());

export { firebaseApp };
export default base;