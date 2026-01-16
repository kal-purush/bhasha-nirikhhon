//MENU TILES
let columns = Math.floor(document.body.clientWidth/50),
rows = Math.floor(document.body.clientHeight/50);

const wrapper = document.getElementById('tiles');

const createTile = index => {
  const tile = document.createElement('div');

  tile.classList.add("tile");

  return tile;
}

const createTiles = quantity =>{
  Array.from(Array(quantity)).map((tile, index) => {
    wrapper.appendChild(createTile(index));
  })
}

const createGrid = () =>{
  wrapper.innerHTML = "";

  columns = Math.floor(document.body.clientWidth/50);
  rows = Math.floor(document.body.clientHeight/85);
  wrapper.style.setProperty("--columns", columns);
  wrapper.style.setProperty("--rows", rows);
  
  createTiles(columns * rows);
}
createGrid();

window.onresize = () => createGrid();


// // Select the menu container element
const menuContainer = document.querySelector('.menu_container');

// // Function to reveal the menu from below
function revealMenu() {
  
  gsap.fromTo(menuContainer, {
     opacity: 0, // Start with opacity 0 (fully transparent)
     y: "100%", // Start with the menu container positioned outside the viewport (to the bottom)
 }, {
     duration: .5,
     opacity: 1,
     y: 0, // Move the menu container to the center (reveal from the bottom)
     ease: "power2.out", // Easing function for smooth animation
 });
 }

// // Call the revealMenu function when the menu icon is clicked or when needed
const menuIcon = document.querySelector('.menu-open');

menuIcon.addEventListener('click', function() {
  revealMenu();
});