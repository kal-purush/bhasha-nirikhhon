;(function () {
  let ctx = document.getElementById("myCanvas").getContext("2d")
  for (let i = 1; i < 10; i++) {
    ctx.moveTo(i * blockSize, 0)
    ctx.lineTo(i * blockSize, 600)
  }
  for (let i = 1; i < 20; i++) {
    ctx.moveTo(0, i * blockSize)
    ctx.lineTo(300, i * blockSize)
  }
  ctx.strokeStyle = color.gray
  ctx.stroke()
})()
;(function () {
  initialize()
})()