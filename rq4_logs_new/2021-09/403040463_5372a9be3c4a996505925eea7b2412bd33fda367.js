'use strict';

const modal = document.querySelector('.modal');
const btnsOpenModal = document.querySelectorAll('.show-modal');
const btnsCloseModal = document.querySelector('.close-modal');
const overlay = document.querySelector('.overlay');
const hideModal = function () {
  modal.classList.add('hidden');
  overlay.classList.add('hidden');
};
const showModal = function () {
  modal.classList.remove('hidden');
  overlay.classList.remove('hidden');
};
for (let i = 0; i < btnsOpenModal.length; i++) {
  btnsOpenModal[i].addEventListener('click', showModal);
}
btnsCloseModal.addEventListener('click', hideModal);
overlay.addEventListener('click', hideModal);
document.addEventListener('keydown', function (eventInfo) {
  if (eventInfo.key === 'Escape' && !modal.classList.contains('hidden')) {
    hideModal();
  }
});