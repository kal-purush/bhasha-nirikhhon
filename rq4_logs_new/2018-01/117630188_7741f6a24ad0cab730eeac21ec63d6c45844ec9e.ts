/*

on collision detection: 
if the player collides with the zombie, and the player's health needs to decrease, and the zombie needs to start attacking,
the zombie and player should both test for collision, so they can modify their own state, and not eachothers.
this is wasteful because now we need to do collision detecting twice
lift collision detection to higher state, and modify state of colliding entities there.
e.g.: 
  for (z of state.zombies) {
    if overlaps(z, state.player) {
      z.touchingplayer = false
      p.touchingzombie = true
      // EDIT: instead of modifying state, just pass as a flag
    }
  }

define initial state
gameLoop(timestamp):
  delta = calcDelta(timestamp)
  detectCollisions()
  newState = newState(delta, input, state)
  draweverything(newState)
  state = newState
  window.requestAnimationFrame(gameLoop)
*/