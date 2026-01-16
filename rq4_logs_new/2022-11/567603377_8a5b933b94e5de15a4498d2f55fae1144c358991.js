gsap.registerPlugin(ScrollTrigger)
let wrap = document.querySelector('.portfolio')

let scrollTween = gsap.to(wrap,{
    // xPercent:-100,
    x:'-1500vw',
    scrollTrigger:{
      trigger:wrap,
      start:'top top',
      end:wrap.offsetWidth,
      scrub:true,
      pin:true,
    }
  })