(function () {
    d3.selection.prototype.containsChild = function (selector) {
        var containsItem = false;
        // TODO: Optimize this.
        this.each(function () {
            if (d3.select(this).child(selector).size() > 0) {
                containsItem = true;
            }
        });
        return containsItem;
    };
})();