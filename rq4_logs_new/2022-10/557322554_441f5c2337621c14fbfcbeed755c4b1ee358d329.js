import { galleryItems } from './gallery-items';
import SimpleLightbox from 'simplelightbox';

import 'simplelightbox/dist/simple-lightbox.min.css';

const galleryContainer = document.querySelector('.gallery');
const getMarkupStringEl = getMarkupString(galleryItems);
galleryContainer.insertAdjacentHTML('beforeend', getMarkupStringEl);

function getMarkupString(galleryItems) {
  return galleryItems
    .map(({ preview, original, description }) => {
      return `<a class = "gallery__item" href = "${original}">
         <img class = "gallery__image" src="${preview}" alt="${description}"></a>`;
    })
    .join('');
}

const lightbox = new SimpleLightbox('.gallery a', {
  captionsData: `alt`,
  captionDelay: 250,
});