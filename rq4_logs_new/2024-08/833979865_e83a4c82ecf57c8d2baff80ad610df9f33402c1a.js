function search_modal() {
	static_backdrop = document.getElementById('staticBackdrop');
	search_car_header = document.getElementById('search-car-header');
		
	if (window.innerWidth <= 576) {
		static_backdrop.classList.add("modal");
		static_backdrop.style.display = "none";
		search_car_header.style.display = "flex";
	}
	else {
		static_backdrop.classList.remove("modal");
		static_backdrop.style.display = "block";
		search_car_header.style.display = "none";
	}
};
document.addEventListener('DOMContentLoaded', () => {	
	search_modal();
});


