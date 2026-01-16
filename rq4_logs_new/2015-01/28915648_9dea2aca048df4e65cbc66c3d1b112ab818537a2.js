/**
 * Created by alexbol on 1/15/2015.
 */
$(document).ready(function() {
    var $dragging = null;

    $(document.body).on("mousemove", function(e) {
        if ($dragging) {
            $dragging.offset({
                top: $dragging.start.top + e.pageY,
                left: $dragging.start.left + e.pageX
            });
        }
    });

    $(document.body).on("mousedown", "div", function (e) {
        $dragging = $(e.target);
        var position = $dragging.position();
        $dragging.start = {
            left: position.left - e.pageX,
            top: position.top - e.pageY
        };
    });

    $(document.body).on("mouseup", function (e) {
        $dragging = null;
    });
});