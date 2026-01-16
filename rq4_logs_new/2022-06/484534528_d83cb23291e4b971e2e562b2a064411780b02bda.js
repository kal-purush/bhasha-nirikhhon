
function caesarCipher(s, k) {
  // Write your code here
let resultArray = [];
  for(let index = 0; index < s.length; index +=1){
    if(s[index] === '-' || s[index] === '!' || s[index] === '_') {
      code  = s.charCodeAt(index)
      resultArray.push(String.fromCharCode(code))
    } else {
      let code = s.charCodeAt(index) + k;
      while(code > 122) {
        code = (code - 122) + 97
      }
      resultArray.push(String.fromCharCode(code))
    }
  }
  return resultArray.join('')
}

console.log(caesarCipher('middle-Outz', 2));
console.log(caesarCipher('Hello_World!', 4));