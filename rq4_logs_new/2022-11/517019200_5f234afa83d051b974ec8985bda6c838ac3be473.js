function Q1(arr) {
  const log = console.log
  for (let index = 0; index < arr.length - 1; index++) {
    let arrow = "-".repeat(2 + index * 3).replace(/(-*)-/, "$1>")
    const memo = index
    log(`index: ${arrow}${index}`)
    log(`before: [${arr.join(", ")}]`)
    if (arr[index] == 0) {
      log(`arr[index]==0`)
      index++
      for (let index2 = arr.length - 1; index2 > index; index2--) {
        arr[index2] = arr[index2 - 1]
      }
      arr[index] = 0
    }
    log(`after:  [${arr.join(", ")}]`)
    if (index != memo) log(`index now is ${index}`)
    log()
  }
}
var arr1 = [1, 0, 2, 3, 0, 4, 5, 0]
var arr2 = Q1(arr1)
// console.log(arr1)
// console.log(arr2)