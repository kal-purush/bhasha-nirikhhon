//Function expression to select element
const selectElement = (s) => document.querySelector(s);
//Openthe menu on click
selectElement(s: '.open').addEventListener('click', () => {
    selectElement(s: '.nav-list').classList.add('active');
});
selectElement()
selectElement(s: '.close').addEventListener('click', () => {
    selectElement(s: '.nav-list').classList.remove('active');
});