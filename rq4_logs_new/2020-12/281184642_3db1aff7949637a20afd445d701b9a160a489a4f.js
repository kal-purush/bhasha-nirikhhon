const addLineBtn = document.querySelector('#add-resource');

let counter = 1;
const limit = 4;

addLineBtn.addEventListener('click', () => {
  const resourcediv = document.querySelector('.resources');
  const messagediv = document.querySelector('.resource-msg');
  if (counter === limit) {
    messagediv.innerHTML = "<p class='add-message'> Easy there! Don't you think 4 resources is enough?</p>";
  } else {
    const newdiv = document.createElement('div');
    newdiv.innerHTML = `<input class='form-control compose' type='text' name='resources${counter + 1}'>`;
    resourcediv.insertAdjacentElement('beforeend', newdiv);
    counter += 1;
  }
});

const addStepBtn = document.querySelector('#add-step');

let stepCounter = 3;
const stepLimit = 8;

addStepBtn.addEventListener('click', () => {
  const resourcediv = document.querySelector('.steps');
  const messagediv = document.querySelector('.step-msg');
  if (stepCounter === stepLimit) {
    messagediv.innerHTML = "<p class='add-message'> Whoa! That's a lotta steps!</p>";
  } else {
    const newdiv = document.createElement('div');
    newdiv.innerHTML = `<div class="step-num">${stepCounter + 1}.</div><textarea class='form-control compose' type='text' name='step${stepCounter + 1}'></textarea>`;
    resourcediv.insertAdjacentElement('beforeend', newdiv);
    stepCounter += 1;
  }
});

$(function() {

  // We can attach the `fileselect` event to all file inputs on the page
  $(document).on('change', ':file', function() {
    var input = $(this),
      numFiles = input.get(0).files ? input.get(0).files.length : 1,
      label = input.val().replace(/\\/g, '/').replace(/.*\//, '');
    input.trigger('fileselect', [numFiles, label]);
  });

  // We can watch for our custom `fileselect` event like this
  $(document).ready(function() {
    $(':file').on('fileselect', function(event, numFiles, label) {

      var input = $(this).parents('.input-group').find(':text'),
        log = numFiles > 1 ? numFiles + ' files selected' : label;

      if (input.length) {
        input.val(log);
      } else {
        if (log) alert(log);
      }

    });
  });

});