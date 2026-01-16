$(document).ready(function () {
  $(".owl-carousel1").owlCarousel({
    loop: true,
    dots: true,
    items: 8,
    autoplay: true,
    autoplayTimeout: 3000,
    autoplayHoverPause: true,
    margin: 10,
    responsiveClass: true,
    responsive: {
      0: {
        items: 1,
        nav: true,
      },
      600: {
        items: 1,
        nav: false,
      },
      1000: {
        items: 1,
        nav: true,
        loop: true,
      },
    },
  });
});
$(document).ready(function () {
  $(".owl-carousel2").owlCarousel({
    loop: true,
    dots: true,
    items: 8,
    autoplay: true,
    autoplayTimeout: 3000,
    autoplayHoverPause: true,
    margin: 10,
    responsiveClass: true,
    responsive: {
      0: {
        items: 2,
        nav: true,
      },
      600: {
        items: 3,
        nav: false,
      },
      1000: {
        items: 4,
        nav: true,
        loop: true,
      },
    },
  });
});
$(document).ready(function () {
  $(".owl-carousel3").owlCarousel({
    loop: true,
    dots: true,
    items: 8,
    autoplay: true,
    autoplayTimeout: 3000,
    autoplayHoverPause: true,
    margin: 30,
    responsiveClass: true,
    responsive: {
      0: {
        items: 1,
        nav: true,
      },
      600: {
        items: 2,
        nav: false,
      },
      1000: {
        items: 3,
        nav: true,
        loop: true,
      },
    },
  });
});
$(window).scroll(function () {
  if ($(this).scrollTop() > 150) {
    $("#fixed-menu").fadeIn();
  } else {
    $("#fixed-menu").fadeOut();
  }
});
var mybutton = document.getElementById("myBtn");

// When the user scrolls down 20px from the top of the document, show the button
window.onscroll = function () {
  scrollFunction();
};

function scrollFunction() {
  if (document.body.scrollTop > 20 || document.documentElement.scrollTop > 20) {
    mybutton.style.display = "flex";
  } else {
    mybutton.style.display = "none";
  }
}

// When the user clicks on the button, scroll to the top of the document
function topFunction() {
  document.body.scrollTop = 0;
  document.documentElement.scrollTop = 0;
}

// window.onload = function () {
//   document.getElementById("loading").style.display = "none";
// };
window.onload = setTimeout(function(){
    document.getElementById("loading").style.display = "none";
 }, 1000);