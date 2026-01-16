window.addEventListener('DOMContentLoaded', (event) => {
  var glide = new Glide('.glide__surf-select', {
    type: 'carousel',
    gap: 16,
    rewind: false,
    perView: 4,
    bound: true,
    peek: {
      before: 0,
      after: 44
    },
    breakpoints: {
      990: {
        perView: 2,
      },
      1200: {
        perView: 3,
      }
    }
  }).mount();
});