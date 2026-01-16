const wrapper = document.querySelector("#wrapper");
const carousel = document.querySelector("#image-carousel");
const images = document.querySelectorAll("#slide_img");
// const btn = document.querySelectorAll('.slide_button')
// const previous = document.querySelector('#prev')
// const nxt = document.querySelector('#next')

images.forEach((slide, index) => {
  slide.style.left = `${index * 100}%`;
});
let counter = 0;

const slideImage = () => {
  images.forEach((e) => {
    e.style.transform = `translateX(-${counter * 100}%)`;
  });
};

const prev = () => {
  if (counter > 0) {
    counter--;
    // console.log(counter);
  } else {
    counter = images.length - 1;
  }
  slideImage();
};
const next = () => {
  if (counter <= images.length - 2) {
    counter++;
    // console.log(counter);
  } else {
    counter = 0;
  }
  slideImage();
};