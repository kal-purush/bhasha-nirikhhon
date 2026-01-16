let hamburgerMenu= document.querySelector('.mobile-menu-svg')
let mobileMenu=document.querySelector('.hide-mobile-menu')
let menuLinks=document.querySelectorAll('.menu-link')


hamburgerMenu.addEventListener('click',function(){
    if(mobileMenu.className==='hide-mobile-menu'){
        console.log('not visible')
        mobileMenu.classList.remove('hide-mobile-menu')
        mobileMenu.classList.add('mobile-menu')
    }
    else if(mobileMenu.className==='mobile-menu'){
        console.log(' visible')
        mobileMenu.classList.add('hide-mobile-menu')
        mobileMenu.classList.remove('mobile-menu')
    }
})

menuLinks.forEach((item)=>{
    item.addEventListener('click',function(){
        mobileMenu.classList.add('hide-mobile-menu')
        mobileMenu.classList.remove('mobile-menu')
    })
})