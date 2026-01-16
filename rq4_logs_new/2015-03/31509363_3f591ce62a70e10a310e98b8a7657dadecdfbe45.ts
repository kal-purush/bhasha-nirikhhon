var simpleboard = function(elem: HTMLElement) {
    this.canDrag = elem.querySelectorAll('[data-can-drag]');
    this.canDrop = elem.querySelectorAll('[data-can-drop]');

    var dragElem = function(target, x, y) {
        var style = window.getComputedStyle(target);
        target.style.left = (x - parseFloat(style.width) * 0.5).toString() + 'px';
        target.style.top = (y - parseFloat(style.height) * 0.5).toString() + 'px';
    }

    var dropElem = function(target, x, y) {

    }

    var touchMoveHandler = function(e) {
        e.preventDefault();

        for (var i = 0; i < e.changedTouches.length; ++i) {
            var touch = e.changedTouches[i];
            var target = touch.target;

            if (target.getAttribute('data-can-drag')) {
                dragElem(target, touch.pageX, touch.pageY);
            }
        }
    }

    var touchEndHandler = function(e) {
        e.preventDefault();

        for (var i = 0; i < e.changedTouches.length; ++i) {
            var touch = e.changedTouches[i];
            var target = touch.target;

            if (target.getAttribute('data-can-drag')) {
                dropElem(target, touch.pageX, touch.pageY);
            }
        }

        if (e.touches.length === 0) {
            document.removeEventListener("touchmove", touchMoveHandler);
            document.removeEventListener("touchend", touchEndHandler);
        }
    }

    var touchStartHandler = function(e) {
        e.preventDefault();

        // safe to call multiple times, will only be added once
        document.addEventListener("touchmove", touchMoveHandler);
        document.addEventListener("touchend", touchEndHandler);
    }

    for (var i = 0; i < this.canDrag.length; ++i) {
        this.canDrag[i].addEventListener("touchstart", touchStartHandler)
    }
}