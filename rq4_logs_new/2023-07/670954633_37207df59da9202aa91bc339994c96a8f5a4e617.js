'use strict';
const switcher =document.querySelector('.btn');
switcher.addEventListener('click', function(){
    document.body.classList.toggle("dark-theme");
    document.body.classList.toggle("light-theme");
    const className = document.body.className;
if(className== 'Light-theme'){
    this.textContent='Dark';
}else{
    this.textContent='Light';
}
console.log('class name '+className);
})