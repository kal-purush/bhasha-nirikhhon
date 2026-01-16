function observable (_value) {
    var value         = _value,
        subscriptions = {},

        DEFAULT_EVT  = 'change',

        fn = function (v) {
            if (arguments.length === 0) {
                return value;
            } else {
                value = v;
                fn.trigger(DEFAULT_EVT, v);
            }
        };

    fn.sub = function (_events, fn) {
        var uniqueId                = Date.now(),
            events;

        subscriptions[uniqueId] = [];

        if (typeof(_events) === "function") {
            fn      = _events;
            _events = DEFAULT_EVT;
        }

        events = _events.match(/\S+\s*/g);

        for (var i = 0; i < events.length; i += 1) {
            subscriptions[uniqueId].push({
                fn    : fn,
                event : events[i].trim()
            });
        }

        return uniqueId;
    };

    fn.unsub = function (uniqueId) {
        delete subscriptions[uniqueId];
    };

    fn.trigger = function (event) {
        for (var uniqueId in subscriptions) {
            for (var ii = 0; ii < subscriptions[uniqueId].length; ii += 1) {
                if (subscriptions[uniqueId][ii].event === event) {
                    subscriptions[uniqueId][ii].fn.apply(fn, [event].concat(Array.prototype.slice.call(arguments, 1)));
                }
            }
        }
    };

    return fn;
}

if (module) module.exports = observable;