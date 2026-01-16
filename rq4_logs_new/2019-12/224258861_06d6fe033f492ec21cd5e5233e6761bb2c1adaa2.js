// Challenge 1

function fetchImages() {
  fetch("https://dog.ceo/api/breeds/image/random/4")
    .then(resp => resp.json())
    .then(imageData => renderImages(imageData))
};

function renderImages(imageData) {
  const imageLinks = imageData.message;
  const frame = document.getElementById('dog-image-container');
  imageLinks.forEach(imageLink => {
    const dogImage = document.createElement('img');
    dogImage.src = imageLink;
    frame.appendChild(dogImage);
  })
};

// Challenge 2

function fetchBreeds() {
  fetch("https://dog.ceo/api/breeds/list/all")
    .then(resp => resp.json())
    .then(breedData => objectifyBreeds(breedData) )
};

function objectifyBreeds(breedData) {
  const breeds = breedData.message;
  renderBreeds(breeds);
  filterBreeds(breeds);
}

function renderBreeds(breeds) {
  const breedList = document.getElementById('dog-breeds');

  for (const key in breeds) {
    const dogBreed = document.createElement('li');
    dogBreed.innerText = key;
    dogBreed.addEventListener('click', textColor);

    // Challenge 3

    function textColor(event) {
      event.currentTarget.style.color = "#009AE4";
    };
    
    if (Array.isArray(breeds[key]) && breeds[key].length) {
      const breedSubList = document.createElement('ul');
      
      breeds[key].forEach(subBreed => {
        const dogSubBreed = document.createElement('li');
        dogSubBreed.innerText = subBreed;
        dogSubBreed.style.color = "#000000";
        dogSubBreed.addEventListener('click', textColor);
        breedSubList.appendChild(dogSubBreed);
      })
      
      dogBreed.appendChild(breedSubList);
    }

    breedList.appendChild(dogBreed);
  }
};

// Challenge 4

function filterBreeds(breeds) {
  let dropdown = document.getElementById('breed-dropdown');
  dropdown.addEventListener('change', (event) => {
    let letter = event.target.value;
    const mainBreeds = Object.keys(breeds);
    let filtered = mainBreeds.filter(mainBreed => mainBreed.startsWith(letter));

  });
};

document.addEventListener('DOMContentLoaded', function() {
  fetchImages();
  fetchBreeds();
});

// ** DO WHAT WORKS NOW - REFACTOR LATER!!