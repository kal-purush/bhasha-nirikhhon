// JavaScript for the homepage example index.html
// Add a current year to the footer
const footerYear = document.querySelector("#year");

function getTime(){
	const date = new Date(),
		yyyy = date.getFullYear();
		
	footerYear.innerHTML = `${yyyy}`;
}

getTime();