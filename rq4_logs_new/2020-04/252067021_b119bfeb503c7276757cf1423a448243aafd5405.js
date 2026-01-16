/**
 * 默认转换进制为10，当没有输入转换进制默认按照10进制
 * 目前转换的字符首位不能包含“-”
 * @param {*} str
 * @param {*} hex
 * @returns
 */
function convertStringToNumber(str, hex = 10) {
  // 处理转换进制是否在2-36之间
  if (typeof hex !== "number" || hex < 2 || hex > 36) {
    return NaN;
  }

  let num = 0;
  let i = 0;

  // 这里要判断str亦即没有传入任何值时应返回0
  if (str) {
    const arr = String(str).split(""); // 防止要转换的值不是字符串
    while (i < arr.length && arr[i] !== ".") {
      const element = arr[i];
      num = num * hex;
      num += element.codePointAt(0) - "0".codePointAt(0);
      i++;
    }

    if (arr[i] === ".") {
      i++;
    }

    let decimal = 1;
    while (i < arr.length) {
      const element = arr[i];
      decimal = decimal / hex;
      num += (element.codePointAt(0) - "0".codePointAt(0)) * decimal;
      i++;
    }
  }

  return num;
}
console.log(convertStringToNumber("1024.0128"));

function convertNumberToString(num, hex = 10) {
  // 处理转换进制是否在2-36之间
  if (typeof hex !== "number" || hex < 2 || hex > 36) {
    throw new Error("radix argument must be between 2 and 36");
  }

  // 处理为输入或者输入为空的情况
  if (arguments.length === 0 || num === "") {
    return '""';
  }
  var str = "";
  // 判断要转换的值是否为number类型
  if (typeof num === "number") {
    // 当要转换的值为0时
    if (num === 0) {
      str = num + ""; // 或者 `${num}`
      return str;
    } else {
      var integer = Math.floor(num); // 整数部分
      var decimal = num - integer; // 小数部分
      while (integer > 0) {
        str = String(integer % hex) + str;
        integer = Math.floor(integer / hex);
      }
      var t = String(decimal).substr(1);
      str = str + t;
    }
  } else {
    str = num;
  }
  return str;
}
console.log(convertNumberToString(0));
console.log(convertNumberToString(undefined));
console.log(convertNumberToString(null));
console.log(convertNumberToString(123.089));
console.log(convertNumberToString("456"));
console.log(convertNumberToString(12348));