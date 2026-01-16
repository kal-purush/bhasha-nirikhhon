AOS.init({
  disable: function () {
    var maxWidth = 1279;
    return window.innerWidth < maxWidth;
  },
});