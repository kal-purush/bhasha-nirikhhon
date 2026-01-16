 

//=============Side Navbar=========
const menuBtn = document.querySelector('.menu :nth-child(2)');
const sideMenu = document.querySelector('.sideNavbar');
const closeMenu = document.querySelector('.closeNav');

menuBtn.addEventListener('click',()=>{
    sideMenu.classList.remove('hideSideNavbar')
    sideMenu.style.display = 'block';
});

closeMenu.addEventListener('click',()=>{ 
    sideMenu.classList.add('hideSideNavbar');  
});




//========Menu IOA============= 
const observed = document.querySelectorAll('.observed');
const goup = document.querySelector('.goup');
const menu = document.querySelector('.menu');

const options = {  
    threshold: 1
}
const observer = new IntersectionObserver((entries,observer)=>{
    entries.forEach(entry=>{
        if(entry.isIntersecting){ 
            menu.classList.remove('fixit-right');
            goup.classList.add("hidegoup")
            console.log(entry);
        }else{
           menu.classList.add('fixit-right');
           goup.classList.remove("hidegoup")

        }
    })
});

observed.forEach(o=>{
    observer.observe(o);
})


//=========Scroll Link==============
const introduction = document.querySelector(".introduction");
const sl = document.querySelector(".scrollLink");
sl.addEventListener("click",()=>{ 
    introduction.scrollIntoView({behavior:"smooth"});
})

//=======Go up=======================
goup.addEventListener("click",()=>{
    document.querySelector("header").scrollIntoView({behavior:"smooth"}); 
})