// how to run: node rxZip.js

var rx = require('rxjs/Rx');

const obs1 = rx.Observable.from([1, 2, 3])
    .map(v => {
        console.log('obs1 element: ' + v);
        return v;
    });
const obs2 = rx.Observable.from([4, 5, 6])
    .map(v => {
        console.log('obs2 element: ' + v);
        return v;
    });
const obs3 = rx.Observable.from([7, 8, 9])
    .map(v => {
        console.log('obs3 element: ' + v);
        return v;
    });
const obs4 = rx.Observable.from(["Leo", "Dani"])
    .map(v => {
        console.log('obs4 element: ' + v);
        return v;
    });

const example = rx.Observable.zip(obs1, obs2, obs3, obs4);

const subscribe = example.subscribe(console.log);