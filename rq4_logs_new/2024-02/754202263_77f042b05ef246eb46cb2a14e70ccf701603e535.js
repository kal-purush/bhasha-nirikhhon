const navBar = document.querySelector('.nav_bar');
const closedNav = document.querySelector('.closed_nav');
const navButton = document.querySelector('.nav_button');

const naveMenu = [
  'HOME',
  'About',
  'service',
  'portfolio',
  'shop',
  'Team',
  'Testimonial',
  'blog',
  'contact',
];

closedNav.innerHTML = naveMenu
  .map(
    e => `<ul>
  <li><a href="#">${e}</a></li>
  </ul>`
  )
  .join('');

function handleNavbar() {
  if (navBar.classList[1] === 'closed_navbar') {
    navBar.classList.remove('closed_navbar');
    navBar.classList.add('opened_navbar');
  } else if (navBar.classList[1] === 'opened_navbar') {
    navBar.classList.remove('opened_navbar');
    navBar.classList.add('closed_navbar');
  }
}

navButton.addEventListener('click', handleNavbar);