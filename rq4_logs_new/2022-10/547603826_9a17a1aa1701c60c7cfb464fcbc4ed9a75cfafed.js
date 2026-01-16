const eatAudio = new Audio("eat.mp3")
class Snake {
    constructor(x, y){
        this.x = x,
        this.y = y
    }
}

const canva = document.querySelector('#canva')
const cnv = canva.getContext('2d')
let speed = 7

let tileDivision = 20
let tileLimit =  600 / tileDivision - 10
let snakeHeadX = 14
let snakeHeadY = 14
let snakeDirX = 0
let snakeDirY = 0
let dirX = 0
let dirY = 0
const parts = []
let snakeLenght = 0
let appleX = 5
let appleY = 5
let score = 0
function loadGame() {

    snakeDirX = dirX
    snakeDirY = dirY
    let GameState = resetGame()
    if(GameState == true) {
        return
    }
    reload()
    loadApple()
    eatFood()
    loadSnake()
    changeDirection()
    loadScore()

    setTimeout(loadGame, 1000 / speed)
}
const resetGame = () => {
    if(snakeHeadX < 0) {
        return true
    }
    else if(snakeHeadX === 30) {
        return true
    }
    else if(snakeHeadY < 0) {
        return true
    }
    else if(snakeHeadY === 30) {
         return true
    }

    for(let i = 0; i < parts.length; i ++) {
        if(snakeHeadX === parts[i].x && snakeHeadY == parts[i].y) {return true}
    }
}

const reload = () => {
    cnv.fillStyle = "#2F4858"
    cnv.fillRect(0,0,canva.width,canva.height)

}
const loadApple = () => {
    cnv.fillStyle = "#F42354"
    cnv.fillRect(appleX * tileDivision, appleY * tileDivision, tileLimit, tileLimit)
}
const eatFood = () => {
    if(snakeHeadX === appleX && snakeHeadY === appleY) {
        eatAudio.play()
        console.log(snakeHeadX, snakeHeadY)
        console.log(appleX, appleY)
        appleX = Math.floor(Math.random() * 30)
        appleY = Math.floor(Math.random() * 30)
        snakeLenght++
        score++
        if(score > 0){
            if(score % 2 == 0) {
                speed ++
            }
        }
    }
}
let loadSnake = () => {
    cnv.fillStyle = "#0FCC3A"
        for(let i = 0; i < parts.length; i++) {
            let item = parts[i]
            cnv.fillRect(item.x * tileDivision, item.y * tileDivision, tileLimit, tileLimit)
        }
        parts.push(new Snake(snakeHeadX, snakeHeadY))
            if(parts.length > snakeLenght) {
                parts.shift()
            }
        cnv.fillStyle = "#F17018"
        cnv.fillRect(snakeHeadX * tileDivision,snakeHeadY * tileDivision, tileLimit, tileLimit)
}
let changeDirection = () => {
    snakeHeadX = snakeHeadX + snakeDirX
    snakeHeadY = snakeHeadY + snakeDirY
}
const loadScore = () => {
    cnv.fillStyle = "white"
    cnv.font = "17px Verdana";
    cnv.fillText(`score: ${score}`, 500, 15)
}
document.addEventListener("keyup", direction)
function direction(e) {
    if(e.keyCode == 38 ) {
        if(dirY == -1) {
            return
        }
        dirY = -1
        dirX = 0
    }
    if(e.keyCode == 40) {
        if(dirY == 1) {
            return
        }
        dirY = 1
        dirX = 0
    }
    if(e.keyCode == 39) {
        if(dirX == 1) {
            return
        }
        dirX = 1
        dirY = 0
    }
    if(e.keyCode == 37) {
        if(dirX == -1) {
            return
        }
        dirX = -1
        dirY = 0
    }
}
loadGame()