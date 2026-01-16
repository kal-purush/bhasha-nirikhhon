let openPopupButtons = document.querySelectorAll('.bttn1');
let form = document.querySelector('#FORM_M');
let popup = document.querySelector('.popup');

let names = document.getElementById('name');
let email = document.getElementById('e-mail');
let massage = document.getElementById('information');
let checkbx = document.getElementById('checkb');






const openPopUp = document.getElementsByClassName('bttn1');
const closePopUp = document.getElementById('close_pop_up');
const popUp = document.getElementById('FORM_M');


closePopUp.addEventListener('click', () => {
    form.classList.remove('open');
    popup.classList.remove('popup_open');
})






function save() {
  localStorage.setItem('Имя', names.value);
  localStorage.setItem('Почта', email.value);
  localStorage.setItem('Сообщение', massage.value);
  if (checkbx.checked) {
    localStorage.setItem('Чекбокс', 1);
  } else {
    localStorage.setItem('Чекбокс', 0);
  }
}
document.addEventListener('DOMContentLoaded', () => {
  names.value = localStorage.getItem('Имя');
  email.value = localStorage.getItem('Почта');
  massage.value = localStorage.getItem('Сообщение');
  let checkBox = localStorage.getItem('Чекбокс');
  if (checkBox == 1) {
    checkbx.checked = true;
  } else if (checkBox == 0) {
    checkbx.checked = false;
  }

  names.oninput = save;
  email.oninput = save;
  massage.oninput = save;
  checkbx.oninput = save;
  
  openPopupButtons.forEach((button) => {
    button.addEventListener('click', (event) => {
      event.preventDefault();
      window.onpopstate = function () {
        window.history.go(0);
        form.classList.remove('open');
        popup.classList.remove('popup_open');
      };
      history.pushState({ page: 1 }, '', '?FORM_M');
      form.classList.add('open');
      popup.classList.add('popup_open');
    });
  });
  
    $(function(){
      $(".forms").submit(function(e){
          e.preventDefault();
          $.ajax({
              type: "POST",
              dataType: "json",
              url: "https://formcarry.com/s/q658vtPcl",
              data: $(this).serialize(),
              success: function (response) {
                if (response.status == 'success') {
                  alert('Успешно!');
                  localStorage.removeItem('Имя');
                  localStorage.removeItem('Почта');
                  localStorage.removeItem('Сообщение');
                 localStorage.removeItem('Чекбокс');
                 names.value = localStorage.getItem('Имя');
                email.value = localStorage.getItem('Почта');
                massage.value = localStorage.getItem('Сообщение');
                checkbx.checked = false;
                }
              },
              error: function (jqxhr, status, errorMsg) {
                alert('Ошибка!');
              },
            });
          });
        });
      });