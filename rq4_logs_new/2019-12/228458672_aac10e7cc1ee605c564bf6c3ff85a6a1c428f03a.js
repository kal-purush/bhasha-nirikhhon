$("multizone, multiZone").each(function () {
  $(this).replaceWith("<div class='" + $(this).attr("class") + "'>" + $(this).html() + "</div>");
});

$("productBroker, productbroker, broker").each(function () {
  $(this).replaceWith("<span class='" + $(this).attr("class") + "'>" + $(this).html() + "</span>");
});

$("productLink, productlink").each(function () {
  $(this).replaceWith("<a class='" + $(this).attr("class") + "' href='#'>" + $(this).html() + "</a>");
});

$("resx").each(function () {
  var cnt = $(this),
    inner = cnt.contents();
  cnt.replaceWith(inner);
});

$(".defaultFacet, .GpcRatingWrapQuery").trigger("templatingCompleted");
$("ul.GpcMenu").trigger("NavigationCountCompleted");
$(".contentHolder").each(function () {
  var cnt = $(this),
  inner = cnt.contents();
  if (cnt.attr("style")) {
    cnt.parent().attr("style", cnt.attr("style"));
  }
  cnt.replaceWith(inner);
});