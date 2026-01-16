const ramenMenu = document.querySelector('#ramen-menu');
const ramenDetail = document.querySelector('#ramen-detail');
const newRamenForm = document.querySelector('#new-ramen');
const apiUrl = 'http://localhost:3000';

// Display all ramen images in #ramen-menu div
function displayRamenImages(ramen) {
  ramen.forEach((ramenItem) => {
    const img = document.createElement('img');
    img.src = ramenItem.image;
    img.alt = ramenItem.name;
    img.dataset.id = ramenItem.id;
    ramenMenu.appendChild(img);
  });
}

// Display ramen details in #ramen-detail div
function displayRamenDetails(ramenItem) {
  ramenDetail.innerHTML = `
    <img src="${ramenItem.image}" alt="${ramenItem.name}">
    <h2>${ramenItem.name}</h2>
    <h3>${ramenItem.restaurant}</h3>
    <p>Rating: ${ramenItem.rating}</p>
    <p>Comment: ${ramenItem.comment}</p>
  `;
}

// Make GET request to retrieve all ramen objects
async function getRamen() {
  const response = await fetch(`${apiUrl}/ramens`);
  const ramen = await response.json();
  displayRamenImages(ramen);
}

// Make GET request to retrieve details for selected ramen object
async function getRamenDetail(id) {
  const response = await fetch(`${apiUrl}/ramens/${id}`);
  const ramenItem = await response.json();
  displayRamenDetails(ramenItem);
}

// Handle click event on ramen image
ramenMenu.addEventListener('click', (event) => {
  if (event.target.tagName === 'IMG') {
    const id = event.target.dataset.id;
    getRamenDetail(id);
  }
});

// Handle submit event on new-ramen form
newRamenForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const data = new FormData(event.target);
  const response = await fetch(`${apiUrl}/ramens`, {
    method: 'POST',
    body: data,
  });
  const newRamenItem = await response.json();
  displayRamenImages([newRamenItem]);
});

// Call getRamen function on page load
getRamen();