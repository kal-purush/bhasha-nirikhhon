document.addEventListener('DOMContentLoaded', () => {
    /* SHOW MENU */
    const navMenu = document.getElementById('nav-menu');
    const navToggle = document.getElementById('nav-toggle');
    const navClose = document.getElementById('nav-close');

    /* Main menu toggle */
    if (navToggle) {
        navToggle.addEventListener('click', () => {
            navMenu.classList.toggle('show-menu');
        });
    }

    /* Close menu */
    if (navClose) {
        navClose.addEventListener('click', () => {
            navMenu.classList.remove('show-menu');
        });
    }

    /*Menu remove*/
    const navLink=  document.querySelectorAll('.nav__link');
    const Linkaction = () => {
        const navMenu = document.getElementById('nav-menu');
        navMenu.classList.remove('show-menu');
    }
    navLink.forEach(n=> n.addEventListener('click',Linkaction));

    /*Shadow for header*/

    const scrollHeader =() =>{
        const header =document.getElementById('header')
        this.scrollY >=50 ? header.classList.add('scroll-header')
                          : header.classList.remove('scroll-header');
    }
    window.addEventListener('scroll',scrollHeader)


    const scrollUp =()=>{
        const scrollUp=document.getElementById('scroll-up');
        this.scrollY >= 350 ? scrollUp.classList.add('show-scroll')
                            :scrollUp.classList.remove('show-scroll');
    }
    window.addEventListener('scroll',scrollUp);
    const sections = document.querySelectorAll("section[id]");
    const navLinks = document.querySelectorAll(".nav__menu a");
  
    function scrollActive() {
      const scrollY = window.scrollY;
  
      sections.forEach(current => {
        const sectionHeight = current.offsetHeight;
        const sectionTop = current.offsetTop - 50; // Adjust this if necessary
        const sectionId = current.getAttribute("id");
        const navLink = document.querySelector(`.nav__menu a[href*=${sectionId}]`);
  
        // Updated condition to include all pixels within the section
        if (scrollY >= sectionTop && scrollY < sectionTop + sectionHeight) {
          navLink.classList.add("active-link");
        } else {
          navLink.classList.remove("active-link");
        }
      });
    }
  
    window.addEventListener("scroll", scrollActive);


    const sr=ScrollReveal({
        origin:'top',
        distance:'60px',
        duration:2500,
        delay:300,
    })

    sr.reveal('.home__Data,.footer')
    sr.reveal('.home__dish',{delay:500,distace:'100px',origin:'bottom'})
    sr.reveal('.home__burger',{delay:1200,distace:'100px',duration:1500})
    sr.reveal('.home__ingredient',{delay:1600,interval:100})
    sr.reveal('.recipe__img, .delivery__img, .contact__image',{origin:'left'})
    sr.reveal('.recipe__data,.delivery__data, .contact__data',{origin:'right'})
    sr.reveal('.popular__card',{interval:100})
    

   

        });  
       
 