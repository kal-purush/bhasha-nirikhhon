function weirdMultiply(sentence) {
    var z = sentence.toString().split('');
    var result = 1;
    if (z < 10) {
      return sentence
    } for (let i = 0;i < z.length; i++) {
      result *= z[i];
    }
    return weirdMultiply(result)
}

console.log(weirdMultiply(39));
console.log(weirdMultiply(999));
console.log(weirdMultiply(3));