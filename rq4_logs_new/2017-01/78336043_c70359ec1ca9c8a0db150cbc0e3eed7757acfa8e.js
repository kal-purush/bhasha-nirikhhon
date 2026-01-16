function ResultAdder() {

};

ResultAdder.prototype.addBox = function (user, languages) {

  var container = document.getElementById('container');
  var box = document.createElement('div');
  box.classList.add('resultbox');
  container.appendChild(box);

  var image = new Image(40,40);
  image.className = 'image';

  var firstLanguage = languages[0];

  if(firstLanguage == 'Java') {
    image.src = '/images/java.png';
    image.alt = "Java";
  } else if (firstLanguage == 'Javascript') {
    image.src = '/images/javascript.png';
    image.alt = 'Javascript';
  } else if (firstLanguage == 'Haskell') {
    image.src = '/images/haskell.png';
    image.alt = 'Haskell';
  } else if (firstLanguage == 'Python') {
    image.src = '/images/python.png';
    image.alt = 'Python';
  } else {
    image.src = '/images/other.png';
    image.alt = 'other';
  }

  var vSpace = document.createElement('div');
  vSpace.classList.add('space');

  var testbox = document.createElement('div');
  testbox.classList.add('testbox');
  testbox.innerHTML = user;
  testbox.appendChild(document.createElement("br"));

  var message = 'My favourite ';
  var numLang = languages.length;
  if(numLang == 1) {
    message += 'language is ' + languages[0] + '!';
  } else {
    message += 'languages are ';
    for(var i = 0; i < numLang; i++) {
      if(i != numLang - 1) {
        message += languages[i] + ', ';
      } else {
        message += 'and ' + languages[i] + '!';
      }
    }
  }
  var messageBox = document.createElement('div');
  messageBox.classList.add('messagebox');
  messageBox.appendChild(document.createTextNode(message));
  testbox.appendChild(messageBox);


  box.appendChild(image);
  box.appendChild(vSpace);
  box.appendChild(testbox);

};