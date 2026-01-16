"use strict";

function getMessage() {
  return document.getElementById('message');
}

function getButton() {
  return document.getElementById('save');
}

function getContainer(){
  return document.getElementById('notes-container');
}

function getElement(id){
  return document.getElementById(id);
}

function getSection(){
  return document.getElementById('notes-section-title');
}

function StickyNoteApp() {
  var   noteMesaggeInput = getMessage(),
        addNoteButton =getButton();

        addNoteButton.addEventListener('click', saveNote);
        noteMesaggeInput.addEventListener('keyup', toggleButton);

        Object.keys(localStorage).forEach(function(key){
          displayNote(key, localStorage[key]);
        });

        window.addEventListener('storage', function(note){
          displayNote(note.key, note.newValue);
        });
}

function  saveNote() {
  var note = getMessage();
  if(note.value) {
    var key = Date.now().toString();
    localStorage.setItem(key,note.value);
    displayNote(key, note.value);
    resetNote(note);
    toggleButton();
  }
}

function resetNote(element) {
  element.value = '';
  element.parentNode.MaterialTextfield.boundUpdateClassesHandler();
  element.blur();
}


function toggleButton(){
  var note = getMessage();
  var button = getButton();
  if(note.value){
    button.removeAttribute('disabled');
  } else {
    button.setAttribute('disabled', 'true');
  }
}


function displayNote(key, message) {
  var container = getContainer();
  var note = getElement(key);
  var par = getSection();

  if(!note) {
    note = document.createElement('sticky-note');
    note.id = key;
    container.insertBefore(note, par.nextSibling);
  }

  if(!message) {
    return note.deleteNote();
  }
  note.setMessage(message);
}




// elemento html sticky-note

var StickyNote = Object.create(HTMLElement.prototype);

StickyNote.TEMPLATE =
  '<div class="message"></div>'+
  '<div class="date"></div>'+
  '<button class="delete mdl-button mdl-js-button mdl-js-ripple-effect">'+
  ' Delete'+
  '</button>';

StickyNote.CLASSES = ['mdl-cell--4-col-desktop', 'mdl-card__supporting-text', 'mdl-cell--12-col',
                        'mdl-shadow--2dp', 'mdl-cell--4-col-tablet', 'mdl-card', 'mdl-cell', 'sticky-note'];

StickyNote.MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'June', 'Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec'];

StickyNote.createdCallback = function() {
  StickyNote.CLASSES.forEach(function(klass) {
    this.classList.add(klass);
  }.bind(this));
  this.innerHTML = StickyNote.TEMPLATE;
  this.messageElement = this.querySelector('.message');
  this.dateElement = this.querySelector('.date');
  this.deleteButton = this.querySelector('.delete');
  this.deleteButton.addEventListener('click', this.deleteNote.bind(this));
};

StickyNote.attributeChangedCallback = function(attributeName) {
  if(attributeName == 'id') {
    var date = new Date();
    if(this.id) {
      date = new Date(parseInt(this.id));
    }
    var month = StickyNote.MONTHS[date.getMonth()];
    this.dateElement.textContent = 'Created on' + month + ' ' + date.getDate();
  }
};

StickyNote.setMessage = function(message) {
  this.messageElement.textContent = message;
  this.messageElement.innerHTML = this.messageElement.innerHTML.replace(/\n/g, '<br>');
};

StickyNote.deleteNote = function() {
  localStorage.removeItem(this.id);
  this.parentNode.removeChild(this);
};

document.registerElement('sticky-note', {
  prototype: StickyNote
});

StickyNoteApp();