const fibFactory = function * () {
  let n1 = 0
  let n2 = 1
  while (true) {
    n3 = n1 + n2
    yield n3
    n1 = n2; n2 = n3
  }
}

const fib = fibFactory()

for (var i=0; i < 10; i++) {
  console.log(fib.next().value)
}