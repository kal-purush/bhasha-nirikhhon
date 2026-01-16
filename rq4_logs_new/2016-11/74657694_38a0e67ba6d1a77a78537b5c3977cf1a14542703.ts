export class Utils {
    range = function(start, stop, step = null) {
        if (arguments.length <= 1) {
            stop = start || 0;
            start = 0;
        }
        step = arguments[2] || 1;

        let length = Math.max(Math.ceil((stop - start) / step), 0);
        let idx = 0;
        let range = new Array(length);

        while(idx < length) {
            range[idx++] = start;
            start += step;
        }

        return range;
    };
}