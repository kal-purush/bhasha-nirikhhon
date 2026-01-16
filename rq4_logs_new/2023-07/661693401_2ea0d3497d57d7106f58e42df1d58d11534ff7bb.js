const nav = document.querySelector('.nav__items');
const navBtn = document.querySelector('.burger-btn');
const allNavItems = document.querySelectorAll('.nav__items__item');
const navBtnBars = document.querySelector('.burger-btn__bars');
const allSections = document.querySelectorAll('.section');
const footerYear = document.querySelector('.footer__year');


const handleNav = () => {
    nav.classList.toggle('nav__items--active')

    navBtnBars.classList.remove('black-bars-color')

    if (!nav.classList.contains('nav__items--active')) {
        handleObserve();
    }

     allNavItems.forEach(item => {
         item.addEventListener('click', () => {
             nav.classList.remove('nav__items--active')
        })
    })

    handleAllItemAnimation();
    deleteAnimation();
       
}

const handleAllItemAnimation = () =>{
    let delayTime =0;

    allNavItems.forEach(item =>{
        item.classList.toggle('nav-items-animation')
        item.style.animationDelay =  delayTime + 's'
        delayTime=delayTime+0.2;
    })
}

const deleteAnimation = () => {
    allNavItems.forEach(item=>{
        item.addEventListener('click',()=>{
            allNavItems.forEach(cancel =>{
                cancel.classList.remove('nav-items-animation')
            })
        })
    })
}

const handleObserve = () => {
    const currentSection = window.scrollY;

    allSections.forEach(section =>{

        if (section.classList.contains('white-section') && section.offsetTop <= currentSection+60){
            navBtnBars.classList.add('black-bars-color')
        } else if (!section.classList.contains('white-section') && section.offsetTop <= currentSection+60){
            navBtnBars.classList.remove('black-bars-color')
        }
    })

    if (nav.classList.contains('nav__items--active')) {
        navBtnBars.classList.remove('black-bars-color')
    }
}

const handleCurrentYear = () => {
    const year = new Date().getFullYear();
    footerYear.innerHTML = year;
}

handleCurrentYear();
navBtn.addEventListener('click', handleNav)
window.addEventListener('scroll', handleObserve)
