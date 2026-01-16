const cursor = document.querySelector(".cursor");
document.addEventListener("mousemove", (e) => {
    cursor.setAttribute('style', 'top: '+e.pageY+'px; let: '+e.pageX+'px;')
    let x = e.pageX;
    let y = e.pagey;

    cursor.style.top = y + "px";
    cursor.style.left = x + "px";
});

const toggleButton = document.getElementsByClassName('toggle-button')[0]
const navbarLinks = document.getElementsByClassName('navbar-links')[0]

toggleButton.addEventListener('click', () => {
  navbarLinks.classList.toggle('active')
})