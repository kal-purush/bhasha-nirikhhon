import SimpleLightbox from "simplelightbox";
import "simplelightbox/dist/simple-lightbox.min.css";


// Add imports above this line
import { galleryItems } from './gallery-items';
// Change code below this line

console.log(galleryItems);

const simpleLightboxList = document.querySelector('.gallery')

const simpleLightboxMarcup = galleryItems.map(({ preview, original, description }) =>
    `<li class="gallery__item">
    <a class="gallery__link" href="${original}" rel="noopener noreferrer">
    <img class="gallery__image" src="${preview}" alt="${description}" />
    </a>
    </li>`
).join('');

simpleLightboxList.innerHTML = simpleLightboxMarcup;

const option = {
    captionsData: "alt",
    captionsDelay: 250
}

const simpleLitebox = new SimpleLightbox('.gallery__link', option);