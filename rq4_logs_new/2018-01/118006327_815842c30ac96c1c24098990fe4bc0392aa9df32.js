'use strict';


let listenForCheck = () => {
  // console.log('listening for check');
  $('.shopping-list').on('click','.shopping-item-toggle', (event) => {
    $(event.target).closest('li').find('.shopping-item').toggleClass('shopping-item__checked');
    // $(event.target).closest('.shopping-item').toggleClass('shopping-item__checked');
  });
};

let addItem = () => {
  const userInput = $('.js-shopping-list-entry').val();
  $('.shopping-list').append(`<li><span class="shopping-item">${userInput}</span><div class="shopping-item-controls"><button class="shopping-item-toggle"><span class="button-label">check</span></button><button class="shopping-item-delete"><span class="button-label">delete</span></button></div></li>`);
  $('.js-shopping-list-entry').val(' ');
};


let listenForSubmit = () => {
  // console.log('now listening for submits');
  $('#js-shopping-list-form').on('submit', (event) => {
    event.preventDefault();
    addItem();
  });
};


let listenForDelete = () => {
  $('.shopping-list').on('click','.shopping-item-delete', (event) => {
    $(event.target).closest('li').remove();
    // console.log('deleted');
  });
};

let initiateQuiz = () => {
  // console.log('quiz initiated');
  listenForSubmit();
  listenForDelete();
  listenForCheck();
};


initiateQuiz();