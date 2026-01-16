const themeSelector = document.querySelector('.calculator__theme button');
const theme = document.querySelectorAll('link')[2];

let positions  = ["9%", "40%", "70%"];
let currentPosition = 0;

themeSelector.parentElement.addEventListener("click", ()=>{
    currentPosition == 2 ? currentPosition = 0: currentPosition += 1;
    themeSelector.style.left = positions[currentPosition];
    theme.setAttribute("href", `css/theme${currentPosition}.css`);
});