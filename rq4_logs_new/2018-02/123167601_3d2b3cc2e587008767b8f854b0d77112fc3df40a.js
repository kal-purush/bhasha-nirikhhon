// This is a simple Tabris.js app. by MR. M. -  Feel free to modify as you please.

const {
  Button,
  TextView,
  ui,
  ImageView,
  AlertDialog,
  app, TextInput
} = require('tabris');

const IMAGE_PATH = 'https://mrmccormack.github.io/imd-learning-tabris/images/';
const MY_GITHUB_REPO = 'https://github.com/mrmccormack/imd-learning-tabris'

// global variables
let numWins = 0;

// Create a text input field with input finished listener

let userText = new TextInput({
  top: 20, left: '20%', right: '20%',
  message: 'Your name: '
}).on('accept', ({text}) => {
  new TextView({
    top: 'prev() 20', left: '20%',
    text: text
  }).appendTo(ui.contentView);
}).appendTo(ui.contentView);

let casinoimage = new ImageView({
  top: 'prev() 10',
  centerX: 0,
  image: IMAGE_PATH + 'casino.jpg'
}).appendTo(ui.contentView);

let button = new Button({
    centerX: 0,
    top: 'prev() 10',
    text: 'Roll Dice'
  })
  .on('select', () => {
    casinoimage.height = 1;
    var rand = 1 + Math.floor(Math.random() * 6);

    image1.image = IMAGE_PATH + rand + '.png';

    if (rand == 6) {
      label.text = userText.text + '- WINNER, you got a 6';
      numWins = numWins + 1 ;
      winnerimage.image = IMAGE_PATH + 'winner.jpg';
    } else {
      label.text = userText.text  + ' - Try again- Wins so far ' + numWins;
      winnerimage.image = '';
    }

    if (numWins == 5){
      label.text = 'You WON with 5 wins!';
      image1.image = IMAGE_PATH + 'whitedice.png';
      winnerimage.image = '';
      numWins = 0;
    }

  }).appendTo(ui.contentView);

// Create a text view and add it too
let label = new TextView({
  centerX: 0,
  top: 'prev() 10',
  font: '14px',
  text: 'Welcome to Mr. M. Casino'
}).appendTo(ui.contentView);

// Display images with different scale modes

let image1 = new ImageView({
  top: 'prev() 10',
  width: 100,
  height: 100,
  centerX: 0,
  scaleMode: 'fill',
  image: IMAGE_PATH + 'whitedice.png'

}).appendTo(ui.contentView);

let winnerimage = new ImageView({
  top: 'prev() 10',
  width: 100,
  height: 100,
  centerX: 0,
  scaleMode: 'fill',
  }).appendTo(ui.contentView);

  let resetbutton = new Button({
    centerX: 0,
    top: 'prev() 10',
    text: 'Reset'
  })
  .on('select', () => {
      casinoimage.height = 109;
      label.text = 'New Game';
      winnerimage.image = '';
      numWins = 0;
      image1.image = 'https://assets-cdn.github.com/images/modules/logos_page/Octocat.png';

  }).appendTo(ui.contentView);



new Button({
  left: 16, top: 'prev() 16', right: 16,
  text: '© INFO'
}).on('select', () => {
  new AlertDialog({
    message: '© 2018 Mr. M. - Free to use',
    buttons: {ok: 'OK'}
  }).open();
}).appendTo(ui.contentView);


  new Button({
  alignment: 'center', centerX: 0,  top: 'prev() 10',
  image: IMAGE_PATH + 'github32.png',
  font: '10px',
  text: 'View source code on my Github Repository'
}).on({
  select: () => app.launch(MY_GITHUB_REPO)
    .then(() => label.text = 'Url has been launched')
    .catch((e) => label.text = e)
}).appendTo(ui.contentView);