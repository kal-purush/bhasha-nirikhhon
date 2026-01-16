const searchButton = document.querySelector('.navigation-link__search');
const modalContainer = document.querySelector('.modal-container');
const modalCloseButton = document.querySelector('.modal-close-button');
const modalForm = document.querySelector('.modal-form');

const isEscapeKey = (evt) => evt.key === 'Escape';

const modalClose = () => {
  modalContainer.classList.add('modal-container-close');
  modalForm.reset();
  document.removeEventListener('keydown', onModalEscClose);
  document.removeEventListener('click', onModalAnyClickClose);
};

function onModalEscClose (evt) {
  if (isEscapeKey(evt)) {
    evt.preventDefault();
    modalClose();
  }
}

function onModalAnyClickClose (evt) {
  if (evt.target === modalContainer) {
    modalClose();
  }
}

searchButton.addEventListener('click', () => {
  modalContainer.classList.remove('modal-container-close');
  document.addEventListener('keydown', onModalEscClose);
  document.addEventListener('click', onModalAnyClickClose);
})

modalCloseButton.addEventListener('click', () => modalClose());