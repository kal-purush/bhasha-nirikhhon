/*
* DOM elements
*/
let homeBtn = $('#home');
let aboutBtn = $('#about');
let workBtn = $('#work');
let blogBtn = $('#blog');
let contactBtn = $('#contact');
let aboutSection = $('#about-section');
let workSection = $('#work-section');
let nameSection = $('#name-section');
let blogSection = $('#blog-section');
let body = $('.container');

let colors = ['#FFEBEE', '#FCE4EC', '#F3E5F5', '#EDE7F6', '#E8EAF6', '#E3F2FD', '#E1F5FE', '#E0F2F1', '#F1F8E9', '#FFF8E1', '#FFFDE7', '#FFF3E0', '#FAFAFA', '#FBE9E7', '#EFEBE9'];
let randomColors = Math.floor(Math.random() * colors.length);


// Hidden about me section on page load

aboutSection.hide();
workSection.hide();




// Button to call about section
aboutBtn.on('click', () => {
    body.css('background-color', '#' + randomColors);
    nameSection.hide();
    aboutSection.fadeIn();
    workSection.hide();
});

// Button to call work section
workBtn.on('click', () => {
  body.css('background-color', randomColors);
  nameSection.hide();
  workSection.fadeIn();
  aboutSection.hide();
});

homeBtn.on('click', () => {
  body.css('background-color', randomColors);
  nameSection.fadeIn();
  workSection.hide();
  aboutSection.hide();
});