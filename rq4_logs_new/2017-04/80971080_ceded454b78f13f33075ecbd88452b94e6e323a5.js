$(function () {
  $(".get_user_recipes").on("click", function(e) {
    e.preventDefault();

    $.get(this.href, function(json) {
      debugger
    });
  });
});