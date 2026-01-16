function gameOver(){
  ctx.font = '20px Arial';
  ctx.fillStyle = 'red';
  ctx.fillText('Game Over!', blocks[Math.round(blockRowCount / 2)][1].x, canvas.height / 2)
}