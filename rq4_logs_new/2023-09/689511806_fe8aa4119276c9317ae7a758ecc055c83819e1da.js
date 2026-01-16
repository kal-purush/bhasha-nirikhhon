import FetchWrapper from './module/fetch-wrapper.js';

const API = new FetchWrapper('https://firestore.googleapis.com/v1/projects/jsdemo-3f387/databases/(default)/documents');

const form = document.querySelector('#create-form');
const name = document.querySelector('#create-name');
const carbs = document.querySelector('#create-carbs');
const protein = document.querySelector('#create-protein');
const fat = document.querySelector('#create-fat');
const message = document.querySelector('.tooltiptext');

form.addEventListener('submit', (e) => {
	e.preventDefault();

	API.post('/heloisa', {
		fields: {
			name: { stringValue: name.value },
			carbs: { integerValue: carbs.value },
			protein: { integerValue: protein.value },
			fat: { integerValue: fat.value },
		},
	}).then((data) => {
		console.log(data);
		if (data.error) {
			// there was an error
			message.classList.add('show');
			return;
		}
		message.classList.remove('show');
		name.value = '';
		carbs.value = '';
		protein.value = '';
		fat.value = '';
	});
});