const canvas = document.getElementById('canvas')
const ctx = canvas.getContext('2d')

let keys = []
let movers = []

for (let i = 0; i < 8; i++) {
    movers.push(new Mover(100 * i + 35, 100, random(0.01, 0.03), random(0.01, 0.03), 3, i))
}

let mouse
let count = 0
let run = true


function main() {
    ctx.globalAlpha = 0.15
    ctx.fillStyle = 'silver'
    ctx.fillRect(0, 0, canvas.clientWidth, canvas.height)
    ctx.globalAlpha = 1

    movers.forEach(e => e.update())
    movers.forEach(e => e.edgeCheck())
    movers.forEach(e => e.draw())

    requestAnimationFrame(main)
}

main()

function random(min, max) {
    let r = Math.random() * (max - min) + min 

    return r
}

document.addEventListener('keydown', e => {
    keys[e.keyCode] = true
})

document.addEventListener('keyup', e => {
    keys[e.keyCode] = false
})

document.addEventListener('click', e => {
    mouse = new PVector(getCursorPosition(canvas, e)[0], getCursorPosition(canvas, e)[1])
    count = 0
    run = true
})