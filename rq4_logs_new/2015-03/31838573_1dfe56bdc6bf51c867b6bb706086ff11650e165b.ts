class Bar {
    constructor() {
        console.log('bar');
    }
}

function bar() {
    new Bar();
}

export = bar;