function solver(power){
  var str = bigInt(2).pow(power).toString();
  var sum = 0;
  for (var i = 0; i < str.length; i++){
    sum += Number(str[i]);
  }
  return sum;
}