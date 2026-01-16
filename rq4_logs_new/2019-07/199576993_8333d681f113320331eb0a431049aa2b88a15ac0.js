console.log('Client side javasript file is cnnected!');

const weatherForm = document.querySelector('form');
const searchInput = document.querySelector('input');
const errorMsg = document.querySelector('#error-msg');
const weatherReportOne = document.querySelector('#forecast-msg-1');
const weatherReportTwo = document.querySelector('#forecast-msg-2');
const weatherReportThree = document.querySelector('#forecast-msg-3');
const weatherReportFour = document.querySelector('#forecast-msg-4');

weatherForm.addEventListener('submit', e => {
  e.preventDefault();
  const address = searchInput.value;
  searchInput.value = '';
  const weatherUrl = 'http://localhost:3000/weather?address=' + address;

  errorMsg.textContent = 'Loading....';
  weatherReportOne.textContent = '';
  weatherReportTwo.textContent = '';
  weatherReportThree.textContent = '';
  weatherReportFour.textContent = '';

  fetch(weatherUrl)
    .then(response => response.json())
    .then(response => {
      if (response.error) {
        errorMsg.textContent = response.error;
      } else {
        errorMsg.textContent = '';
        weatherReportOne.textContent = 'Summary: ' + response.forecast;
        weatherReportTwo.textContent =
          'Temperature (In Far.): ' + response.currentTemperature;
        weatherReportThree.textContent = 'Location: ' + response.location;
        weatherReportFour.textContent = 'Search Term: ' + response.searchTerm;
      }
    });
});