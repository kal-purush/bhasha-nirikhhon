$(".pokeball_cover").hover(function() {
  if ($(this).height() === 128) {
    $(this).css({
      height: "2em",
      width: "2em"
    });
  } else if ($(this).height() < 50) {
    $(this).css({
      height: "8em",
      width: "8em"
    });
  }
});