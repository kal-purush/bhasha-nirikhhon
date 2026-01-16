const methods = [
    (req0, res0, next) => { console.log("methods0"); next(); }, 
    (req1, res1, next) => { console.log("methods1"); next(); }
];

var j = 0;
function makechain(methods) {
    chain = () => {};
    len = methods.length;
    for (var i = len - 1 ; i >= 0; i--) {
        let curr = methods[i];
        let next = chain;
        // console.log(next.toString());
        chain = (req, res) => {
            if(++j > 10)  throw Error();
            curr(req, res, () => next(req, res));
        }
    }

    return chain;
}

chain = makechain(methods)
chain();

// # i = 1
// ## before
// chain = () => {}
// ## After
// chain = (req, res) {
//     methods[1](req, res, () => () => {})
// }
//
// # i = 0
// ## before
// chain = (req, res) { methods[1](req, res, () => () => {}) }
// ## After
// chain = (req, res) {
//     methods[0](req, res, (req, res) => { methods[1](req, res, () => () => {}) })
// }