$(function () {
  const trigger = $(".js-accordion-title");
  const text = $(".accordion__content");
  const NotFirstText = $(".accordion__content:not(:first)");
  
  NotFirstText.hide();

  trigger.on("click", function () {
    $(this).next(text).slideToggle();
    $(text).not($(this).next(text)).slideUp();
  });
});