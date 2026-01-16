function id(length = 100000) {
  return Date.now() + '-' + Math.floor(Math.random() * length)
}

module.exports = id