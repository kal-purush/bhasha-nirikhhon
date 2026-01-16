const hello = (function () {
    console.log("chicken")
})();

hello

// function test(){
//     test = function(){};
//     console.log("test");
// }

// test(); // "test"
// test(); //

var test = (function() {
    var executed = false;
    return function() {
        if (!executed) {
            executed = true;
            console.log("test");
        }
    };
})();

test(); // "test"
test(); 