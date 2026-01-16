$(".animationText").not(".slick-initialized").slick({
  autoplay: true,
  autoplaySpeed: 1500,
  vertical: true,
  slidesToShow: 1,
  slidesToScroll: 1,
  adaptiveHeight: true,
  arrows: false,
});

$(".shortSlider").not(".slick-initialized").slick({
  autoplay: false,
  autoplaySpeed: 4000,
  dots: true,
  slidesToShow: 1,
  slidesToScroll: 1,
  arrows: false,
  // centerMode: true,
});
$(".sliderImagesBig").slick({
  autoplay: false,

  dots: false,
  slidesToShow: 1,
  slidesToScroll: 1,
  arrows: false,
});

$(".relayCardWrapper0")
  .not(".slick-initialized")
  .slick({
    // dots: true,
    // prevArrow: ` <button class="leftArrow">
    //   <i class="fas fa-chevron-left"></i>
    //   </button>`,
    // nextArrow: '<i class="fas fa-chevron-right"></i>',
    autoplay: true,
    arrows: false,
    autoplaySpeed: 2000,
    // centerMode: true,
    centerPadding: "0px",
    slidesToShow: 3,
    responsive: [
      {
        breakpoint: 900,
        settings: {
          arrows: false,
          // centerMode: true,
          centerPadding: "1rem",
          slidesToShow: 3,
        },
      },
      {
        breakpoint: 768,
        settings: {
          // centerMode: true,
          centerPadding: "60px",
          slidesToShow: 1,
        },
      },
      {
        breakpoint: 480,
        settings: {
          centerMode: true,
          centerPadding: "0px",
          slidesToShow: 1,
        },
      },
    ],
  });

$(".relayers .relayArrows .leftArrow").click(function () {
  $(".relayCardWrapper0").slick("slickPrev");
});
$(".relayers .relayArrows .rightArrow").click(function () {
  $(".relayCardWrapper0").slick("slickNext");
});

$(".relayCardWrapper1")
  .not(".slick-initialized")
  .slick({
    autoplay: true,
    autoplaySpeed: 2000,
    arrows: false,
    // centerMode: true,
    centerPadding: "0px",
    slidesToShow: 3,
    responsive: [
      {
        breakpoint: 900,
        settings: {
          arrows: true,
          // centerMode: true,
          centerPadding: "1rem",
          slidesToShow: 3,
        },
      },
      {
        breakpoint: 768,
        settings: {
          // centerMode: true,
          centerPadding: "60px",
          slidesToShow: 1,
        },
      },
      {
        breakpoint: 480,
        settings: {
          centerMode: true,
          centerPadding: "0px",
          slidesToShow: 1,
        },
      },
    ],
    arrows: true,
    dots: false,
  });
$(".mentors .relayArrows .leftArrow").click(function () {
  $(".relayCardWrapper1").slick("slickPrev");
});
$(".mentors .relayArrows .rightArrow").click(function () {
  $(".relayCardWrapper1").slick("slickNext");
});

var collapseBtn = document.querySelectorAll(".titleAndButton .fas");

for (let i = 0; i < collapseBtn.length; i++) {
  collapseBtn[i].parentElement.addEventListener("click", () => {
    if (!collapseBtn[i].classList.contains("open")) {
      collapseBtn[i].classList.add("open");
      for (let j = 0; j < collapseBtn.length; j++) {
        if (i != j) {
          collapseBtn[j].classList.remove("open");
        }
      }
    } else {
      collapseBtn[i].classList.remove("open");
    }
  });
}
var faqArrow = document.querySelectorAll(".questionAndArrow span .fas");

for (let i = 0; i < faqArrow.length; i++) {
  faqArrow[i].parentElement.addEventListener("click", () => {
    if (!faqArrow[i].classList.contains("open")) {
      // faqArrow[i].parentElement.previousElementSibling.style.color = "#fff";
      faqArrow[i].classList.add("open");
      for (let j = 0; j < faqArrow.length; j++) {
        if (i != j) {
          // faqArrow[j].parentElement.previousElementSibling.style.color =
          // "#f37049";
          faqArrow[j].classList.remove("open");
        }
      }
    } else {
      // faqArrow[i].parentElement.previousElementSibling.style.color = "#f37049";
      faqArrow[i].classList.remove("open");
    }
  });
}

// NAVBAR

$(document).ready(function () {
  $(".menu-icon").on("click", function () {
    $("nav ul").toggleClass("showing");
  });
});

// Scrolling Effect

jQuery(function ($) {
  var $nav = $("header");
  var $win = $(window);
  var winH = $win.height(); // Get the window height.

  $win
    .on("scroll", function () {
      if ($(this).scrollTop() > winH) {
        $nav.addClass("showNav");
      } else {
        $nav.removeClass("showNav");
      }
    })
    .on("resize", function () {
      // If the user resizes the window
      winH = $(this).height(); // you'll need the new height value
    });
});