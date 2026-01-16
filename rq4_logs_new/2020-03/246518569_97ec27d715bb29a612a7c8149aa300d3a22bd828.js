(() => {
    const {
        AsyncArray,
        add,
        subtract,
        multiply,
        divide,
        mod,
        less,
        equal,
        lessOrEqual,
        sqrt
    } = Homework;

    const promisify = (fn) => {
        return function(...args) {
            return new Promise((resolve, reject) => {
                fn.call(this, ...args, (res) => {resolve(res)});
            });
        }
    }

    window.HomeworkAsync = {
        AsyncArray: function(initial) {
            const a = new AsyncArray(initial);

            return {
                set: promisify(a.set),
                push: promisify(a.push),
                get: promisify(a.get),
                pop: promisify(a.pop),
                length: promisify(a.length),
                print: a.print
            }
        },
        add: promisify(add),
        subtract: promisify(subtract),
        multiply: promisify(multiply),
        divide: promisify(divide),
        mod: promisify(mod),
        less: promisify(less),
        equal: promisify(equal),
        lessOrEqual: promisify(lessOrEqual),
        sqrt: promisify(sqrt)
    }

    Object.freeze(window.HomeworkAsync);
})();