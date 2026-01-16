'use strict';
(function (recalculations, $, undefined) {
    var ctx, w, h;

    recalculations.init = function (zmEl, width, height) {
        ctx = zmEl.getContext('2d');
        w = width;
        h = height;
    };


    recalculations.redrawByRecalculateBlocks = function (blocksNum) {
        var obj = state.rawData;
        ctx.clearRect(0, 0, w, h);
        var str = models.buildClassStructure(obj);
        var pkgs = models.collapseByPackage(obj);
        str = models.buildXIndex(w, str);
        var threads = models.buildThreads(obj, str);
        var tme = models.getTimeBorders(threads);
        threads = models.setThreadY(threads, tme.tmin, tme.tmax, h);
        state.visibleStr = str;
        state.visibleThrs = threads;

        drawItems.drawclzz(ctx, str);
        drawItems.drawTime(ctx, tme.tmin, tme.tmax);
        drawItems.drawThreads(ctx, threads);
    };

    recalculations.redrawByZoom = function (startX, startY, endX, endY) {
        var obj = state.rawData;
        ctx.clearRect(0, 0, w, h);
        var str = models.getClassStructure(state.visibleStr, startX < endX ? startX : endX, startX > endX ? startX : endX);
        str = models.recalculateX(str, w, startX < endX ? startX : endX, startX > endX ? startX : endX);
        state.visibleStr = str;
        drawclzz(ctx, str);

        var threads = models.filterThreads(state.visibleThrs, startY, endY, startX, endX);
        if (threads.length != 0) {
            threads = models.updateThreadX(threads, str);
            var tme = models.getTimeBorders(threads);
            threads = models.setThreadY(threads, tme.tmin, tme.tmax, h);
            state.visibleThrs = threads;

            drawItems.drawTime(ctx, tme.tmin, tme.tmax);
            drawItems.drawThreads(ctx, threads);
        }
    };

    recalculations.redrawByTimer = function () {
        var obj = state.rawData;
        ctx.clearRect(0, 0, w, h);
        var pkgs = models.collapseByPackage(obj);
        var str = models.buildClassStructure(obj);
        str = models.buildXIndex(w, str);
        var threads = models.buildThreads(obj, str);
        var tme = models.getTimeBorders(threads);
        threads = models.setThreadY(threads, tme.tmin, tme.tmax, h);
        state.visibleStr = str;
        state.visibleThrs = threads;

        drawItems.drawclzz(ctx, str);
        drawItems.drawTime(ctx, tme.tmin, tme.tmax);
        drawItems.drawThreads(ctx, threads);
    };
}(window.recalculations = window.recalculations || {}, jQuery));