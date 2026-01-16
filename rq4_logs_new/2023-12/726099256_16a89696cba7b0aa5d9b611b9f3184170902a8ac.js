// Part 1: Getting Started
// Explore the provided code to familiarize yourself with important aspects
// such as current DOM structure, element IDs, and available CSS classes.
// Take five minutes to familiarize yourself with CSS Custom Properties 
// (variables) - they are an amazing addition to CSS. 
// If you are familiar with using variables with SASS/LESS pre-processors, 
// CSS Custom Properties are similar but far more powerful because they are dynamic 
// (their values can be changed during runtime) - and they are built into the CSS language!
// Start the project by building a main content element using the following steps:
// Select and cache the <main> element in a variable named mainEl.
const mainEl = document.querySelector('main');
// Set the background color of mainEl to the value stored in the --main-bg CSS custom property.
// Hint: Assign a string that uses the CSS var() function like this: 'var(--main-bg)'.
mainEl.style.backgroundColor = 'var(--main-bg)';
// Set the content of mainEl to <h1>DOM Manipulation</h1>.
mainEl.innerHTML = '<h1>DOM Manipulation</h1>';
// Add a class of flex-ctr to mainEl.
// Hint: Use the Element.classList API.
mainEl.classList.add('flex-ctr');
// Part 2: Creating a Menu Bar
// Next, create a blank menu bar that we can use to later add some interactivity to the page:
// Select and cache the <nav id="top-menu"> element in a variable named topMenuEl.
const topMenuEl = document.querySelector('#top-menu');
// Set the height of the topMenuEl element to be 100%.
topMenuEl.style.height = '100%';
// Set the background color of topMenuEl to the value stored in the --top-menu-bg CSS custom property.
topMenuEl.style.backgroundColor = 'var(--top-menu-bg)';
// Add a class of flex-around to topMenuEl.
topMenuEl.classList.add('flex-around');
// Part 3: Adding Menu Buttons
// Copy the following data structure to the top of your index.js file; you will use it to create your menu buttons.
// If this data was provided by an external source, it would allow that source to control how our menu is built. 
// We would simply implement the logic, and allow the data to decide what displays. 
// This is not typically done with menus, but it can be done with any DOM element.
// Menu data structure

let menuLinks = [
    { text: 'about', href: '/about' },
    { text: 'catalog', href: '/catalog' },
    { text: 'orders', href: '/orders' },
    { text: 'account', href: '/account' },
];

// Select the topMenuEl element
document.querySelector('#top-menu');
// Iterate over the menuLinks array
menuLinks.forEach(link => {
    // Create an <a> element
    const menuLink = document.createElement('a');
    // Set the href attribute
    menuLink.setAttribute('href', link.href);
    // Set the content/text of the <a> element
    menuLink.textContent = link.text;
    // Append the <a> element to topMenuEl
    topMenuEl.appendChild(menuLink);
});