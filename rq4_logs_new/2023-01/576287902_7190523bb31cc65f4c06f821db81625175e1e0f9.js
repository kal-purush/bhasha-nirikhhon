function getHiddenNav() {
	return document.querySelector('nav div#navbarResponsive');
}


document.addEventListener('DOMContentLoaded', function () {

	const btnNav = document.querySelector('button.navbar-toggler');
	btnNav.addEventListener('click', function (e) {
		const nav = getHiddenNav();
		nav.classList.toggle('collapse');
		e.stopPropagation();
	});

	document.addEventListener('click', function (e) {
		const nav = getHiddenNav();
		nav.classList.add('collapse');
	});

	getHiddenNav().addEventListener('click', function (e) {
		e.stopPropagation();
	});
});