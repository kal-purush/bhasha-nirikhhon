'use strict';
(function () {
  var form = document.querySelector('.form');
  var formInput = form.querySelector('.form__input'),
      formSubmit = form.querySelector('.form__submit');

  formInput.addEventListener('change', function () {
    var file = formInput.files[0];
    var fileName = document.querySelector('.subheading');

    fileName.textContent = file.name;
  });


  form.addEventListener('submit', function (e) {
    var req, fd;
    var maxSize = parseInt(formInput.getAttribute('data-max-size'));

    e.preventDefault();

    // Validate file size before uploading to server
    // if (formInput.files[0].size > maxSize) {
    //   var fileName = document.querySelector('.subheading');
    //   fileName.textContent = 'File size must be equal or less than 200KB';
    //   return;
    // }

    formSubmit.value = 'Uploading ...';
    formSubmit.setAttribute('disabled', true);
    formInput.setAttribute('disabled', true);

    fd = new FormData();
    fd.append('file', formInput.files[0]);

    req = new XMLHttpRequest();
    req.upload.addEventListener('progress', function (pe) {
      if (pe.lengthComputable) {
        var percentage = pe.loaded / pe.total;
        var progressBar = document.querySelector('.progress-bar');
        var progressWidth = Math.round(percentage * progressBar.clientWidth);

        progressBar.style.boxShadow = progressWidth + 'px 0 0 0 #1DBA9A inset';
      }
    });

    req.open('POST', '/', true);
    req.onreadystatechange = function() {
      if (req.readyState === 4 && req.status === 200) {
        location.href = req.responseURL;
      }
    }

    req.send(fd);
  });
}());