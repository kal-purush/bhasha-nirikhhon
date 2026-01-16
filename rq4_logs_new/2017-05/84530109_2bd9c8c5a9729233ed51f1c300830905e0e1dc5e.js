$(function () {
    var year = (new Date()).getFullYear(), $year = $("footer .year");
    //noinspection EqualityComparisonWithCoercionJS
    if (year != $year.html()) {
        $year.append(" - " + year);
    }

    $("#toggle-menu").click(function () {
        $("#menu, #content").parent().toggleClass("active");
    });

    function init() {
        if ($("#toggle-menu").is(":visible")) {
            $("#menu, #content").parent().addClass("off-canvas");
        } else {
            $("#menu, #content").parent().removeClass("off-canvas");
        }
    }

    $(window).resize(init);
    init();
});