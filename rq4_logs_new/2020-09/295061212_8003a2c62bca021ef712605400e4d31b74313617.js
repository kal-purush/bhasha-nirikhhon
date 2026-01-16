const ipAddressInput = document.querySelector('#ip-address-input');
const sendButton = document.querySelector('#send-button');
const ipAddress = document.querySelector('#ip-address');
const locationId = document.querySelector('#location');
const timezone = document.querySelector('#timezone');
const isp = document.querySelector('#isp');

const map = L.map('map');

const setMap = (lat, lng) => {
  map.setView([lat, lng], 13);

  const locationIcon = L.icon({
    iconUrl: 'images/icon-location.svg',

    iconSize: [40, 50], // size of the icon
    iconAnchor: [20, 50], // point of the icon which will correspond to marker's location
  });

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  }).addTo(map);

  L.marker([lat, lng], { icon: locationIcon }).addTo(map);
};

const setData = (data) => {
  ipAddress.innerText = data.ip;
  locationId.innerText = `${data.location.city}, ${data.location.region}`;
  timezone.innerText = `UTC ${data.location.timezone}`;
  isp.innerText = data.isp;

  const lat = data.location.lat;
  const lng = data.location.lng;

  setMap(lat, lng);
};

const fetchLocation = async (domain) => {
  let url =
    'https://geo.ipify.org/api/v1?apiKey=at_DZ1VdYQwNiUJXhND5O2Y3DSM6qDyt';

  if (domain) url = url.concat(`&domain=${domain}`);

  return fetch(url)
    .then((response) => response.json())
    .then((data) => setData(data));
};

window.addEventListener('load', () => {
  fetchLocation();
});

sendButton.addEventListener('click', () => {
  if (ipAddressInput.value) fetchLocation(ipAddressInput.value);
});

ipAddressInput.addEventListener('keyup', (event) => {
  if (event.keyCode === 13) fetchLocation(ipAddressInput.value);
});