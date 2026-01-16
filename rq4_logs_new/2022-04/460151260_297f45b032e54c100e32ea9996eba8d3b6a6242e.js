import { FILE_TYPES } from './constants.js';

const fileChooser = document.querySelector('#upload-file');
const photoPreview = document.querySelector('.img-upload__preview > img');

fileChooser.addEventListener('change', () => {
  const file = fileChooser.files[0];
  const fileName = file.name.toLowerCase();

  const match = FILE_TYPES.some((it) => fileName.endsWith(it));

  if(match) {
    photoPreview.src = URL.createObjectURL(file);
  }
});