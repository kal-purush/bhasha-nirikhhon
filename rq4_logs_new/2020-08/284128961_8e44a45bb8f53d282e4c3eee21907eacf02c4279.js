/**
 * 
 * Manipulating the DOM exercise.
 * Exercise programmatically builds navigation,
 * scrolls to anchors from navigation,
 * and highlights section in viewport upon scrolling.
 * 
 * Dependencies: None
 * 
 * JS Version: ES2015/ES6
 * 
 * JS Standard: ESlint
 * 
*/

// Define global variables
const mainElement = document.querySelector('main');
const navbarMenu = document.querySelector('#navbar__list');

// Dynamically add sections
for (let i = 4; i <= 7; i++) {
  const sectionElement = document.createElement('section');
  const landingElement = document.createElement('div');
  
  sectionElement.appendChild(landingElement);

  landingElement.setAttribute('class', 'landing__container');
  sectionElement.setAttribute('id', `section${i}`);
  sectionElement.setAttribute('data-nav', `Section ${i}`);

  landingElement.insertAdjacentHTML('beforeend', `<h2>Section ${i}</h2>`);
  landingElement.insertAdjacentHTML('beforeend', '<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Morbi fermentum metus faucibus lectus pharetra dapibus. Suspendisse potenti. Aenean aliquam elementum mi, ac euismod augue. Donec eget lacinia ex. Phasellus imperdiet porta orci eget mollis. Sed convallis sollicitudin mauris ac tincidunt. Donec bibendum, nulla eget bibendum consectetur, sem nisi aliquam leo, ut pulvinar quam nunc eu augue. Pellentesque maximus imperdiet elit a pharetra. Duis lectus mi, aliquam in mi quis, aliquam porttitor lacus. Morbi a tincidunt felis. Sed leo nunc, pharetra et elementum non, faucibus vitae elit. Integer nec libero venenatis libero ultricies molestie semper in tellus. Sed congue et odio sed euismod.</p>');
  landingElement.insertAdjacentHTML('beforeend', '<p>Aliquam a convallis justo. Vivamus venenatis, erat eget pulvinar gravida, ipsum lacus aliquet velit, vel luctus diam ipsum a diam. Cras eu tincidunt arcu, vitae rhoncus purus. Vestibulum fermentum consectetur porttitor. Suspendisse imperdiet porttitor tortor, eget elementum tortor mollis non.</p>');

  mainElement.appendChild(sectionElement);
}

// Dynamically build navbar
const selectSectionElements = document.querySelectorAll('section');

selectSectionElements.forEach(section => {
  navbarMenu.insertAdjacentHTML('beforeend', `<li><a href="${section.id}">${section.dataset.nav}</a></li>`)
});

const navbarList = document.querySelectorAll('li');

navbarList.forEach(item => {
  item.setAttribute('class', 'menu__link');
});

// Add and remove active className
const addActive = (inView, section, item) => {
  if (inView) {
    section.classList.add('your-active-class');
    item.classList.add('link__active');
  }
}
const removeActive = (section, item) => {
  section.classList.remove('your-active-class');
  item.classList.remove('link__active');
}

// Detect and apply active className
const activeSection = () => {
  navbarList.forEach(item => {
    const section = document.getElementById(item.firstChild.getAttribute('href'));
    const sectionOffset = (Math.floor(section.getBoundingClientRect().top));
    
    removeActive(section, item);
    addActive((sectionOffset < 200 && sectionOffset >= -200), section, item);
  })
}
window.addEventListener('scroll', activeSection);

// Scroll to anchor ID with ScrollTo
document.querySelectorAll('a').forEach(a => {
  a.addEventListener('click', function(e) {
      e.preventDefault();
      
      document.getElementById(a.getAttribute('href')).scrollIntoView({
        behavior: 'smooth'
      });
  });
});