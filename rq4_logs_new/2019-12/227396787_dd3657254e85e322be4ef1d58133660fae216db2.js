// Firebase App (the core Firebase SDK) is always required and
// must be listed before other Firebase SDKs
var firebase = require('firebase/app');
// Add the Firebase products that you want to use
require('firebase/database');
//var ReactDOM = require('react-dom');

const firebaseConfig = {
	apiKey: 'AIzaSyD92Ce4HzvYMeqJFvFSdEe3yHNPMYXrs9k',
	authDomain: 'herstory-bd9f1.firebaseapp.com',
	databaseURL: 'https://herstory-bd9f1.firebaseio.com',
	projectId: 'herstory-bd9f1',
	storageBucket: 'herstory-bd9f1.appspot.com',
	messagingSenderId: '916711912288',
	appId: '1:916711912288:web:bcc7df1fc28ae093ece405',
	measurementId: 'G-414R0D8YD9'
};

export function FirebaseInit() {
	firebase.initializeApp(firebaseConfig);

	var database = firebase.database();
	var ref = database.ref('questions');
	ref.on('value', gotData, errData);
	function gotData(data) {
		console.warn(data.val());
		console.warn(typeof data.val());
	}
	function errData(err) {
		console.warn('Error!');
		console.warn(err);
	}
}