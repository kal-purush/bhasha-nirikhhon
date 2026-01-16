function isValid(str) {
  if (str.length === 1) return false;
  var stack = [];
  var hash = { ")": "(", "}": "{", "]": "[" };
  for (let i = 0; i < str.length; i++) {
    if (!hash[str[i]]) {
      stack.push(str[i]);
    } else {
      var temp = stack.pop();
      if (hash[str[i]] === temp) {
        continue;
      } else {
        return false;
      }
    }
  }
  return stack.length ? false : true;
}
var isValid = function(s) {
  var array = [];
  s = s.split("");
  for (let i = 0; i < s.length; i++) {
    if (s[i] === "(") array.push(")");
    else if (s[i] === "[") array.push("]");
    else if (s[i] === "{") array.push("}");
    else if (s.length === 0 || array[array.length - 1] !== s[i]) return false;
    else array.pop();
  }
  return array.length === 0;
};
// IsValid???