module.exports = function(ac, b) {
  var fn = function(b) {
    var a = ac[0] || ''
    var c = ac[1] || ac[0] || ''
    return a + b + c
  }

  return b ? fn(b) : fn
};