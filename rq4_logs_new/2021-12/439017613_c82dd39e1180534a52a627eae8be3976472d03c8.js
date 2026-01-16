//Extend ECMA-262 syntax into a superset of JSON.
//JSON syntax is defined by ECMA-404 and permanently fixed by RFC 7159, but the DoubleStringCharacter and
// SingleStringCharacter productions of ECMA-262 can be extended to allow unescaped U+2028 LINE SEPARATOR and U+2029 PARAGRAPH SEPARATOR characters.

const LS=""
const PS=eval("'\u2029'")
console.log(LS)
console.log(PS)