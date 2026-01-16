//Last modified// set current year in footer
const currentDate = new Date();
document.querySelector('#year').textContent = currentDate.getFullYear();

//Last modified
document.querySelector('#lastModified').innerHTML = `Last updated: ${document.lastModified}`;