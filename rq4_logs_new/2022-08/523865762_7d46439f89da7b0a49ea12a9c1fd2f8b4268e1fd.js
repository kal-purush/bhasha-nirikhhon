import { galleryItems } from './gallery-items.js';
// Change code below this line

console.log(galleryItems);

const imgContainer = document.querySelector(".gallery");
const imgGallery = createImagesMarkup(galleryItems);
imgContainer.insertAdjacentHTML('beforeend', imgGallery);

function createImagesMarkup(galleryItems) {
    return galleryItems.map(({ preview, original, description }) => {
        return `
    <div class="gallery__item">
      <a class="gallery__link" href="${original}">
        <img
          class="gallery__image"
          src="${preview}"
          data-source="${original}"
          alt="${description}"
        />
      </a>
    </div>
`
    }).join('');
}

imgContainer.addEventListener("click", onImgClick);

function onImgClick(evt) {
    evt.preventDefault();
    if (!evt.target.classList.contains('gallery__image')) {
        return;
        
    }
    
    const imgUrl = evt.target.dataset.source;
    
    const instance = basicLightbox.create(`
    <img src="${imgUrl}">
`);

    instance.show();

}


