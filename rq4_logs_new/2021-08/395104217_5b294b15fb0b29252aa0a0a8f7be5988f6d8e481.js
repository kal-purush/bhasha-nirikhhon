let btnmenuMobile = document.querySelector('.menuSecond div');
let btnClosemenuMobile = document.querySelector('.closeMenuMobile div');
let menuMobile = document.querySelector('.menuMobile');
let oppacity = document.querySelector('.oppacity');
let opened = true;
btnmenuMobile.addEventListener('click', openMenu);
btnClosemenuMobile.addEventListener('click', openMenu);

function openMenu(){
  if(opened){
    menuMobile.style.width = '50vw';
    oppacity.style.width = '50vw';
    opened = false;
  }else{
    menuMobile.style.width = '0vw';
    oppacity.style.width = '0vw';
    opened = true;
  }
}
