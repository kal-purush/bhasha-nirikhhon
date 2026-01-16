(function() {
  const uploadFiles = document.querySelector('.cloud-status__card--passport');

  if (!uploadFiles) {
    return;
  }

  const file1 = uploadFiles.querySelector('input[name="passport-photo"]');
  const file2 = uploadFiles.querySelector('input[name="person-with-passport-photo"]');
  const clearFile = uploadFiles.querySelectorAll('.file-load__clear');
  const sendBtn1 = uploadFiles.querySelector('.cloud-status__card-btn--desktop');
  const sendBtn2 = uploadFiles.querySelector('.cloud-status__card-btn--mobile');

  const isFormFilled = function() {
    let filled = true;

    if (file1.value === '' || file2.value === '') {
      filled = false;
    }

    return filled;
  }

  const checkInputs = function() {
    if (isFormFilled()) {
      sendBtn1.hasAttribute('disabled')
      && sendBtn1.removeAttribute('disabled');
      sendBtn2.hasAttribute('disabled')
      && sendBtn2.removeAttribute('disabled');
    }

    if (!isFormFilled()) {
      !sendBtn1.hasAttribute('disabled')
      && sendBtn1.setAttribute('disabled', '');
      !sendBtn2.hasAttribute('disabled')
      && sendBtn2.setAttribute('disabled', '');
    }
  }

  const onFileClear = function() {
    if (!sendBtn1.hasAttribute('disabled')) {
      sendBtn1.setAttribute('disabled', '');
    }
    if (!sendBtn2.hasAttribute('disabled')) {
      sendBtn2.setAttribute('disabled', '');
    }
  }

  window.onPassportFileDrop = function() {
    checkInputs();
  };

  file1.addEventListener('change', window.onPassportFileDrop);
  file2.addEventListener('change', window.onPassportFileDrop);
  clearFile.forEach(function(item) {
    item.addEventListener('click', onFileClear);
  });

})();