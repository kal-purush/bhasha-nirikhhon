'use strict';

(function () {
  var TYPE_FiLES = ['gif', 'jpg', 'jpeg', 'png'];
  var inputFileAvatar = document.querySelector('#avatar');
  var userAvatar = document.querySelector('.ad-form-header__preview > img');

  inputFileAvatar.addEventListener('change', function () {
    var file = inputFileAvatar.files[0];
    var fileName = file.name.toLowerCase();

    var matches = TYPE_FiLES.some(function (it) {
      return fileName.endsWith(it);
    });

    if (matches) {
      var reader = new FileReader();

      reader.addEventListener('load', function () {
        userAvatar.src = reader.result;
      });

      reader.readAsDataURL(file);
    }
  });

  var inputFilePhotos = document.querySelector('#images');
  var photoContainer = document.querySelector('.ad-form__photo-container');
  var userPhotos = document.querySelector('.ad-form__photo');

  var photo = function (address) {
    var photoImage = document.createElement('img');
    var photo = userPhotos.cloneNode(true);
    var photoImage = document.createElement('img');
    var photoItem = photoImage.cloneNode(true);
    photoContainer.appendChild(photo);
    photo.appendChild(photoItem);
    photoItem.style.width = 60 + 'px';
    photoItem.style.height = 60 + 'px';
    photoItem.style.margin = 5 + 'px', 5 + 'px';
    photoItem.style.objectFit = 'contain';
    photoItem.src = address;
  };
  userPhotos.remove();

  inputFilePhotos.addEventListener('change', function () {
    var file = inputFilePhotos.files[0];
    var fileName = file.name.toLowerCase();

    var matches = TYPE_FiLES.some(function (it) {
      return fileName.endsWith(it);
    });

    if (matches) {
      var reader = new FileReader();

      reader.addEventListener('load', function () {
        photo(reader.result);
      });

      reader.readAsDataURL(file);
    }
  });

})();