const scrollRevealOption = {
  distance: "50px",
  origin: "bottom",
  duration: 1000,
};

// header container
ScrollReveal().reveal(".header__container img", {
  ...scrollRevealOption,
  origin: "right",
});

ScrollReveal().reveal(".header__container h1", {
  ...scrollRevealOption,
  delay: 500,
});

ScrollReveal().reveal(".header__container .btn", {
  ...scrollRevealOption,
  delay: 1000,
});

ScrollReveal().reveal(".header__container a", {
  ...scrollRevealOption,
  delay: 1500,
});

// service container
ScrollReveal().reveal(".services__card", {
  ...scrollRevealOption,
  interval: 500,
});

// projects container
ScrollReveal().reveal(".projects__card", {
  ...scrollRevealOption,
  interval: 500,
});

// banner container
ScrollReveal().reveal(".banner__content", scrollRevealOption);

// blogs container
ScrollReveal().reveal(".blogs__card", {
  ...scrollRevealOption,
  interval: 500,
});