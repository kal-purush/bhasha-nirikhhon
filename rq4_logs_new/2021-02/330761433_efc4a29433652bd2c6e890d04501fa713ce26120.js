const popupForm = document.querySelector('.visit-form');
const searchButton = document.querySelector('.button-search');

const arrivalDate = popupForm.querySelector('[name=date_arrival]');
let adultNumber = popupForm.querySelector('[name=adult_number]');
let kidsNumber = popupForm.querySelector('[name=kids_number]');
let isStorageSupport = true;
let adultStorage = '';
let kidsStorage = '';

try {
  adultStorage = localStorage.getItem('adultNumber');
  kidsStorage = localStorage.getItem('kidsNumber');
} catch (err) {
  isStorageSupport = false;
}

searchButton.addEventListener('click', function () {
  popupForm.classList.add('visit-form-open');
  arrivalDate.focus();
  if (adultStorage || kidsStorage) {
    adultNumber.value = adultStorage;
    kidsNumber.value = kidsStorage;
  }
});

window.addEventListener('keydown', function (evt) {
  if (evt.keyCode === 27) {
    if (popupForm.classList.contains('visit-form-open')) {
      evt.preventDefault();
      popupForm.classList.remove('visit-form-open');
    }
  }
});

popupForm.addEventListener('submit', function (evt) {
  evt.preventDefault();
  console.log('Отправляем форму');
  if (!adultNumber.value || !kidsNumber.value) {
    console.log('Нужно ввести количесво взрослых и детей');
    popupForm.classList.add('modal-error');
  } else {
    if (isStorageSupport) {
      localStorage.setItem('adultNumber', adultNumber.value);
      localStorage.setItem('kidsNumber', kidsNumber.value);
    }
  }
});