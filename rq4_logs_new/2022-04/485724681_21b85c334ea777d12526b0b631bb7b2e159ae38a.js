// function win() {
//      let s= screen.width;
//      return s;
// }

// window.alert(win());

const img = document.querySelectorAll('.img-board');
console.log(img);

img.forEach(imgs => {
    imgs.addEventListener('click', () => {
        removeActiveClasses();
        imgs.classList.add('active');
    });
});

//to remove active classes 
function removeActiveClasses() {
    img.forEach(imgs => {
        imgs.classList.remove('active');
    });
}