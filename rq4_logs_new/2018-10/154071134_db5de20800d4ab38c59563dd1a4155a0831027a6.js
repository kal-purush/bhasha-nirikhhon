$(window).scroll(function(){
  const header = $('.header');
  const scroll = $(window).scrollTop();
  const hero = $(".hero");
  const nav = $("nav")

  const html = "<nav><a href='#'>About</a><a href='#'>Resumé</a><a href='#'>Portfolio</a><a href='#'>Blog</a><a href='#'>Home</a><a href='#'>Contact</a></nav>"

  const unscrolledHeader = "<nav><div class='side-nav left-nav'><a href='#'>About</a><a href='#'>Resumé</a><a href='#'>Portfolio</a></div><div class='side-nav'><h3>full stack web developer & designer</h3></div><div class='side-nav right-nav'><a href='#'>Blog</a><a href='#'>Home</a><a href='#'>Contact</a></div></nav>"

  if (scroll >= 300) {
    header.html(html)
    header.addClass('fixed') 
    $("nav").addClass("scrolled-nav")
    hero.addClass("shift-hero");
  }
  else {
    header.removeClass('fixed');
    hero.removeClass("shift-hero");
    header.html(unscrolledHeader)
}
});



