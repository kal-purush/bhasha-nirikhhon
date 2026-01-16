'use strict';
//---------------hw----------------
const patternName = /^[A-ZА-ЯЁ][a-zа-яё]{1,32} [A-ZА-ЯЁ]\.[A-ZА-ЯЁ]\.$/;
const patternUserName = new RegExp('^[A-ZА-ЯЁ][a-zа-яё]{1,32} ([A-ZА-ЯЁ]\.){2}$');
//---------------------------------
const usersArray = [];
const textForm = document.getElementById('textForm');
const messages = document.getElementById('messages');

textForm.addEventListener('submit', (event) => {
  event.preventDefault();
  const {
    target: {
      textInput: { value: valueTextInput },
    },
  } = event;
  const prepareValue = valueTextInput.trim();
  if (patternName.test(prepareValue) && !usersArray.includes(prepareValue)) {
    event.target.textInput.style.backgroundColor = '';
    usersArray.push(prepareValue);
    const pMessage = createElement(
      'p',
      {},
      document.createTextNode(prepareValue)
    );
    messages.append(pMessage);
    console.log(usersArray);
    textForm.reset();
  } else {
    event.target.textInput.style.backgroundColor = 'pink';
    event.target.textInput.value = '';
  }
});

const str =
  'The test() 1956method of RegExp instances executes a search with this regular 20expression for a match between a regular expression and a specified string. Returns 546true if there is a match; false otherwise.';

const pattern2 = /\b[a-z]{3}\b/gi;

const pattern3 = new RegExp("(\\d+)([a-z]+)", "g");

const replaseStr = str.replace(pattern3, '$2 $1');