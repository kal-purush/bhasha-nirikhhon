function binarySearch(num, arr) {
    let min = 0;
    let max = arr.length - 1;
    let mid = Math.floor(arr.length / 2);
    while (num !== arr[mid]) {
      mid = Math.floor((min + max) / 2);
      if (num < arr[mid]) {
        max = mid;
      } else if (num > arr[mid]) {
        min = mid;
      }
    }
    return mid;
}
function recursiveBinarySearch(num, arr, min = 0, max = arr.length) {
    let mid = Math.floor((min + max) / 2);
    if (num == arr[mid]) {
      return mid;
    }
    if (num < arr[mid]) {
      return recursiveBinarySearch(num, arr, min, mid);
    }
    return recursiveBinarySearch(num, arr, mid, max);
}
console.log(recursiveBinarySearch(2,[1,2,3,4,5,6,8]));


  function palindrom(str){
    str = str.split("");
    let i = 0;
    let i2 = str.length -1;
    while(i !== Math.floor(str.length/2)){
        let char = str[i]
        str[i] = str[i2]
        str[i2] = char
        i++
        i2--
    }
    return str.join('')
  }
  console.log(palindrom('Arno'));