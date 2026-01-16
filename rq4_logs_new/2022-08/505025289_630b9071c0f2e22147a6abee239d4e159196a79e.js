function catchMe(el){
el.style.top=getRandom(0,(window.innerHeight-el.offsetHeight))+'px';
el.style.left=getRandom(0,(window.innerWidth-el.offsetWidth))+'px';
}
function getRandom(min,max){
    min=Math.ceil(min);
    max=Math.floor(max);
    return Math.floor(Math.random()*(max-min));
}