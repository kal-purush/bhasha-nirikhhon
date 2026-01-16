
const formRules = {
  name: {
    rule: /^^[a-zA-Z\s\d]+$/,
    msg: '不能有特殊符號',
  },
  age: {
    rule: /^\d{2}$/,
    msg: '只能是 2 碼數字',
  },
  email: {
    rule: /^\w+((-\w+)|(\.\w+))*\@[A-Za-z0-9]+((\.|-)[A-Za-z0-9]+)*\.[A-Za-z]+$/,
    msg: '格式錯誤',
  },
  phone: {
    rule: /^09\d{8}$/,
    msg: '格式錯誤，須為 09 開頭的 10 碼數字',
  },
}

// 驗證欄位資料
$('#reserveName').on('input propertychange', function() {
  const data = {
    input: $(this),
    inputValue: $(this).val(),
    rule: formRules.name.rule,
    errorMsg: formRules.name.msg,
  }
  validationInputFn(data);
});
$('#reserveAge').on('input propertychange', function() {
  const data = {
    input: $(this),
    inputValue: $(this).val(),
    rule: formRules.age.rule,
    errorMsg: formRules.age.msg,
  }
  validationInputFn(data);
});
$('.js-form-email').on('input propertychange', function() {
  const data = {
    input: $(this),
    inputValue: $(this).val(),
    rule: formRules.email.rule,
    errorMsg: formRules.email.msg,
  }
  validationInputFn(data);
});
$('.js-form-phone').on('input propertychange', function() {
  const data = {
    input: $(this),
    inputValue: $(this).val(),
    rule: formRules.phone.rule,
    errorMsg: formRules.phone.msg,
  }
  validationInputFn(data);
});


// 訂課資料
$('.js-reserve-data').on('change', function() {
  let datepicker = {};
  datepicker = getDatepicker();
  checkInputValue();
});


function validationInputFn(data) {
  const borderStyle = ['border-danger', 'animate__animated', 'animate__headShake'];
  const { input, inputValue, rule, errorMsg } = data;
  const inputName =  $(input).attr('name');
  const errorMsgContent = $(input).next();

  if (!rule.test(inputValue)){
    input.addClass(borderStyle);
    errorMsgContent.addClass('d-block');

    if (inputValue === '') {
      $(errorMsgContent).text(`${inputName} 為必填`);
    } else {
      $(errorMsgContent).text(`${inputName} ${errorMsg}`);
    }

  } else {
    input.removeClass(borderStyle);
    errorMsgContent.removeClass('d-block').text('');
  }
}

function getDatepicker() {
  let inputData = {
    muscleEndurance: [],
    userData: {},
  }

  inputData.datepicker = changeDate();
  inputData.isDoYoga = $('input[name="practiseYogaRadio"]:checked').val();
  inputData.sportsTime = $('input[name="sportsTime"]:checked').val();
  inputData.muscleEndurance = getProblemBetter(getDatepicker.muscleEndurance);
  inputData.userData.name = $('#reserveName').val();
  inputData.userData.age = $('#reserveAge').val();
  inputData.userData.gender = $('#reserveGender').val();
  inputData.userData.email = $('#reserveEmail').val();
  inputData.userData.phone = $('#reservePhone').val();

  return inputData;
}
function checkInputValue() {
  const datepicker = changeDate();
  const isDoYoga = $('input[name="practiseYogaRadio"]:checked').val();
  const muscleEndurance = $('input[name="problemBetterCheckbox"]:checked').val();
  const sportsTime = $('input[name="sportsTime"]:checked').val();
  const formReserve = document.querySelectorAll('.js-form-reserve');
  const reserveArray = [isDoYoga, muscleEndurance, sportsTime, datepicker];
  const reserveBtn =  $('.js-form-reserve-btn');

  formReserve.forEach(function(items) {
    reserveArray.push(items.value);
  });

  reserveArray.forEach(function(input) {
    if (
      input === undefined
      || input === ''
      || input === NaN
      || input === null
    ) {
      reserveBtn.removeClass('btn-primary-light').addClass('btn-outline-primary-light disabled');

    } else {
      reserveBtn.removeClass('btn-outline-primary-light disabled').addClass('btn-primary-light');
    }
  });
}

function changeDate() {
  const date = $('#datepicker').val().split('/');
  return new Date(`${date[2]}/${date[0]}/${date[1]}`) / 1000;
}
function getProblemBetter() {
  let valueArray = [];
  $('input[name="problemBetterCheckbox"]').each(function() {
    if ( $(this).prop('checked') ) {
      valueArray.push($(this).val());
    }
  });

  return valueArray;
}




/*-------------------bs5驗證------------------- */
// Example starter JavaScript for disabling form submissions if there are invalid fields
(function () {
  'use strict'

  // Fetch all the forms we want to apply custom Bootstrap validation styles to
  var forms = document.querySelectorAll('.needs-validation')

  // Loop over them and prevent submission
  Array.prototype.slice.call(forms)
    .forEach(function (form) {
      form.addEventListener('submit', function (event) {
        if (!form.checkValidity()) {
          event.preventDefault()
          event.stopPropagation()
        }

        form.classList.add('was-validated')
      }, false)
    })
})()