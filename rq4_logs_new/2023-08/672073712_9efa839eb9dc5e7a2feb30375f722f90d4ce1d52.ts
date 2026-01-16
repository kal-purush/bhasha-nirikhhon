import { Point } from './shared'
import { GraphicsConfig, GameConfig } from './interface'

class Snake {
  public parts: Array<Point> = []

  constructor(
    public length: number,
    public radius: number,
    public speed: number,
    x: number,
    y: number
  ) {
    for (let _ = 0; _ < length; _++) {
      this.parts.push(new Point(x, y))
      x -= 2 * this.radius
    }
  }

  move(goal: Point, radius: number): boolean {
    let smallestDistancePartIndex = 0
    let smallestDistance = Math.max(window.innerHeight, window.innerWidth)
    for (let i = 0; i < this.length; i++) {
      const distance = goal.subtract(this.parts[i]).norm()
      if (distance < smallestDistance) {
        smallestDistancePartIndex = i
        smallestDistance = distance
      }
    }

    if (smallestDistance + this.speed < this.radius + radius)
      return false

    const head = this.parts[smallestDistancePartIndex]
    const dir = goal.subtract(head).normed().mul(this.speed)
    head.x += dir.x
    head.y += dir.y

    let i = smallestDistancePartIndex
    for (let _ = 0; _ < this.length - 1; _++) {
      const prev = this.parts[i % this.length]
      const curr = this.parts[(i + 1) % this.length]
      const dist = prev.subtract(curr).norm()
      if (dist > 2 * this.radius + this.speed) {
        const dir = prev.subtract(curr).normed().mul(this.speed)
        curr.x += dir.x
        curr.y += dir.y
      } else {
        const dir = prev.subtract(curr).normed().mul(2 * this.radius)
        curr.x = prev.x - dir.x
        curr.y = prev.y - dir.y
      }
      i++
    }

    return true
  }
}

class Player {
  constructor(
    public speed: number,
    public radius: number,
    public x: number,
    public y: number
  ) {}

  move(direction: boolean[]): void {
    if (direction[0]) // up
      if (this.y >= this.speed + this.radius)
        this.y -= this.speed
    if (direction[1]) // right
      if (this.x + this.speed + this.radius <= window.innerWidth)
        this.x += this.speed
    if (direction[2]) // down
      if (this.y + this.speed + this.radius <= window.innerHeight)
        this.y += this.speed
    if (direction[3]) // left
      if (this.x >= this.speed + this.radius)
        this.x -= this.speed
  }
}

class SnakeGameGraphics {

  constructor(
    public config: GraphicsConfig
  ) { }

  private drawPoint(point: Point, radius: number, color: string): void {
    this.config.context.fillStyle = color
    this.config.context.beginPath()
    this.config.context.arc(point.x, point.y, radius, 0, 2 * Math.PI)
    this.config.context.fill()
  }

  private drawSnake(snake: Snake): void {
    for (let i = 0; i < snake.length; i++) {
      this.drawPoint(snake.parts[i], snake.radius, this.config.snakeColor)
    }
  }

  private drawPlayer(player: Player): void {
    this.drawPoint(new Point(player.x, player.y),
      player.radius, this.config.playerColor)
  }

  clear(): void {
    this.config.context.clearRect(
      0, 0,
      this.config.canvas.width,
      this.config.canvas.height
    )
  }

  draw(snake: Snake, player: Player): void {
    this.drawSnake(snake)
    this.drawPlayer(player)
  }

  move(snake: Snake, player: Player, direction: boolean[]): boolean {
    this.clear()
    const moved = snake.move(new Point(player.x, player.y), player.radius)
    if (!moved) return false
    player.move(direction)
    this.draw(snake, player)
    return true
  }
}

export class SnakeGameHTML {
  public graphics: SnakeGameGraphics
  public snake: Snake
  public player: Player
  private direction: boolean[] = [false, false, false, false]
  public inProgress: boolean = false
  private interval?: number

  constructor (
    public config: GameConfig
  ) {
    this.graphics = new SnakeGameGraphics(config.graphics)
    this.snake = this.snakeFromConfig()
    this.player = this.playerFromConfig()
  }

  private setDirectionFromKey(key: string, setTo: boolean): void {
    switch (key) {
      case 'ArrowUp':
        this.direction[0] = setTo
        break
      case 'ArrowRight':
        this.direction[1] = setTo
        break
      case 'ArrowDown':
        this.direction[2] = setTo
        break
      case 'ArrowLeft':
        this.direction[3] = setTo
        break
    }
  }

  private snakeFromConfig(): Snake {
    return new Snake(
      this.config.snake.length,
      this.config.snake.radius,
      this.config.snake.speed,
      this.config.snake.x,
      this.config.snake.y
    )
  }

  private playerFromConfig(): Player {
    return new Player(
      this.config.player.speed,
      this.config.player.radius,
      this.config.player.x,
      this.config.player.y
    )
  }

  private setEventListeners(): void {
    const self = this
    window.addEventListener('keydown', function(e) {
      self.setDirectionFromKey(e.key, true)
    }, false)
    window.addEventListener('keyup', function(e) {
      self.setDirectionFromKey(e.key, false)
    }, false)
  }

  start(): void {
    this.inProgress = true
    this.setEventListeners()
    this.snake = this.snakeFromConfig()
    this.player = this.playerFromConfig()
    this.graphics.draw(this.snake, this.player)
    this.interval = setInterval(this.update.bind(this), 1000 / this.config.framesPerSec)
  }

  update(): void {
    const moved = this.graphics.move(this.snake, this.player, this.direction)
    if (!moved) this.end(false)
  }

  end(gameWon: boolean): void {
    this.inProgress = false
    this.graphics.clear()
    clearInterval(this.interval)
    // @ts-ignore (callback is in the html file)
    onEndGame(gameWon)
  }
}