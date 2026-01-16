//import classnamer and types
import classnamer, {
    ClassNameObject,
    ClassNamePrimitive,
    ClassNameFragment,
    ClassNameFragmentList } from "./../dist/classnamer";

// simple examples
console.log(classnamer("super", "man")); // => "super man"
console.log(classnamer("super", { man : true })); // => "super man"
console.log(classnamer({ super: true }, { man : true })); // => "super man"

// lots of arguments of various types
console.log(classnamer("super", { man: true, krypton: false }, "zor", { el: true })); // => "super man zor el"

// other falsy values are just ignored
console.log(classnamer(null, false, "super", undefined, 0, 1, { man: null }, "")); // => "super 1"

// complex fragments will be recursively flattened
let fragments: ClassNameFragmentList = ["man", { kripton: true, phantom: false }];
console.log(classnamer("super", fragments)); // => "super man kripton"