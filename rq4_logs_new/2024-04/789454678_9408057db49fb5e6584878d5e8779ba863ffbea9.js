// Add imports above this line
import { galleryItems } from './gallery-items';
// Change code below this line
import SimpleLightbox from 'simplelightbox';
import 'simplelightbox/dist/simple-lightbox.min.css';

let list = document.querySelector('.gallery');

function createImagesGallery() {
  for (const image of galleryItems) {
    const listElement = document.createElement('li');
    listElement.classList.add('gallery__item');

    const a = document.createElement('a');
    a.classList.add('gallery__link');
    a.setAttribute('href', image.original);
    a.dataset.source = image.original;

    const img = document.createElement('img');
    img.classList.add('gallery__image');
    img.setAttribute('src', image.preview);
    img.setAttribute('alt', image.description);

    list.append(listElement);
    listElement.append(a);
    a.append(img);

    a.addEventListener('click', handler);
    function handler(event) {
      event.preventDefault();
      img.src = image.original;
    }
  }
  const lightBox = new SimpleLightbox(`.gallery a`, {
    captionsData: 'alt',
    captionDelay: 250,
  });
}

createImagesGallery();

console.log(galleryItems);