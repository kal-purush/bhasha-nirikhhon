
function insert(num) {
  document.form.textvieuw.value = document.form.textvieuw.value + num
}

function equal() {
  var exp = document.form.textvieuw.value
  if (exp) {
    document.form.textvieuw.value = eval(document.form.textvieuw.value)
  }
}

function clean() {
  document.form.textvieuw.value = ''
}

function back() {
  var exp = document.form.textvieuw.value
  document.form.textvieuw.value = exp.substring(0, exp.length-1)
}