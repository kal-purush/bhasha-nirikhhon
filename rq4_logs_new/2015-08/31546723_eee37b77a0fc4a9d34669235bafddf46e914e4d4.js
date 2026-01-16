'use strict';

module.exports = function(accessor_multi, plot, plotMixin) {  // Injected dependencies
    return function() { // Closure function
        var p = {},  // Container for private, direct access mixed in variables
            fieldNames = accessor_multi.fieldNames;
        var pathLines = fieldNames.map(function(x){
                    var pl = plot.pathLine();
                    pl.classname = x;
                    return pl;
                });

        function multiline(g) {
            var group = plot.groupSelect(g, plot.dataMapper.array, p.accessor.d);

            for(var i=0; i<fieldNames.length; ++i) {
                multiline.interaction(group.selection.append('path').attr('class', fieldNames[i]));
            }

            multiline.refresh(g);
        }

        multiline.refresh = function(g) {
            refresh(g, pathLines);
        };

        function binder() {
            for(var i=0; i<fieldNames.length; ++i) {
                pathLines[i].init(p.accessor.d, p.xScale, p.accessor[fieldNames[i]], p.yScale);
            }
        }

        // Mixin 'superclass' methods and variables
        plotMixin(multiline, p).plot(accessor_multi, binder);
        binder();

        return multiline;
    };
};

function refresh(g, pathLines) {
    for(var i=0; i<pathLines.length; ++i) {
        g.selectAll('path.' + pathLines[i].classname).attr('d', pathLines[i]);
    }
}