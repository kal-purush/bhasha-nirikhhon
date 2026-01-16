function toggleNavigation() {
	const navigation = document.getElementById('mainNavigation');

	if (navigation.classList.contains('isExpanded')) 
	{
		console.log(1)
		navigation.classList.remove('isExpanded');
	} else {
		console.log(2)
		navigation.classList.add('isExpanded')
	}
}