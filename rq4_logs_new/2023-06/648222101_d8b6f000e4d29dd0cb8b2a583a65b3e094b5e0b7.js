const hero = document.querySelector('.hero');
const text = document.querySelector('h1');
let walk = 100;
function shadow(e) {
    const {offsetWidth: width, offsetHeight: height } = hero;
    let {offsetX: x, offsetY: y} = e;

   
    if(this !==e.target){
        x = x + e.target.offsetLeft;
        y = y + e.target.offsetTop;
    }
    const xwalk = (x / width * walk) - (walk / 2);
    const ywalk = (y /height * walk) - (walk / 2);

    text.style.textShadow = `${xwalk}px ${ywalk}px 0 rgba(255,0,255,0.7),
    ${xwalk * -1}px ${ywalk * -1}px 0 rgba(0,255,255,0.7)
    `
   
}

hero.addEventListener('mousemove',shadow);