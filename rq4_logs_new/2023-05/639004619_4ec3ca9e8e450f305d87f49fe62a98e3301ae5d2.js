import './bootstrap';
//Add SCSS
import '~resources/scss/app.scss';
// Import all of Bootstrap's JS
import * as bootstrap from 'bootstrap';
//Vite process assets
import.meta.glob([
	'../img/**',
	'../fonts/**',
]);