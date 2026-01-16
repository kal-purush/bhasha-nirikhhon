chairs = [
  {
    link: 'https://assets.codepen.io/1462889/ch1.png',
    color: '#8c3845',
  },
  {
    link: 'https://assets.codepen.io/1462889/ch2.png',
    color: '#262626',
  },
  {
    link: 'https://assets.codepen.io/1462889/ch3.png',
    color: '#486378',
  },
  {
    link: 'https://assets.codepen.io/1462889/ch4.png',
    color: '#486378',
  },
  {
    link: 'https://assets.codepen.io/1462889/ch5.png',
    color: '#877a72',
  },
  {
    link: 'https://assets.codepen.io/1462889/ch6.png',
    color: '#656968',
  },
];

// Selector veriavbles
const body = document.querySelector('body');
const btns = document.querySelectorAll('.btn');
const chairImg = document.querySelector('.product');
btns.forEach((btn, i) => {
  btn.addEventListener('click', () => {
    body.style.backgroundColor = chairs[i].color;
    chairImg.src = chairs[i].link;
  });
});