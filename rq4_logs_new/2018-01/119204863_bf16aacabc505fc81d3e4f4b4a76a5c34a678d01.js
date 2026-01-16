'use strict';

class Vector{ //  Позволяет контролировать расположение объектов в двумерном пространстве и управляет их размером и перемещением.
  constructor(x = 0, y = 0){
    this.x = x;
    this.y = y;
  }
  plus(vector){ // Создает и возвращает новый объект типа Vector, координаты которого будут суммой соответствующих координат суммируемых векторов.
      if(vector instanceof Vector){
        return new Vector(this.x + vector.x, this.y + vector.y);
      }
      else{
        throw  new Error (`Можно прибавлять к вектору только вектор типа Vector`);
      }
  }
  times(n){ // Создает и возвращает новый объект типа Vector, координаты которого будут равны соответствующим координатам исходного вектора, умноженным на множитель.
    return new Vector(this.x * n, this.y * n);
  }
}

class Actor{ // контролирует все движущиеся объекты на игровом поле и контролирует их пересечение.
  constructor(pos = new Vector(0, 0), size = new Vector(1, 1), speed = new Vector(0, 0)){
    this.pos = pos; // расположение
    this.size = size; // размер
    this.speed = speed; // скорость
    if(!(this.pos instanceof Vector) || !(this.size instanceof Vector) || !(this.speed instanceof Vector)){
      throw new Error (`Расположение, размер и скорость должны быть объектом Vector`);
    }
  }

  get type(){
    return 'actor';
  }
  act(){

  }
  get left(){
    return this.pos.x;
  }
  get right(){
    return this.pos.x + this.size.x;
  }
  get top(){
    return this.pos.y;
  }
  get bottom(){
    return this.pos.y + this.size.y;
  }
  isIntersect(actor){
    if(!(actor instanceof Actor) || (!actor)){
      throw new Error (`Не является экземпляром Actor или не передано аргументов`);
    }
    if(actor === this){ // если равен самому себе
      return false;
    }
    if(this.right > actor.left &&
    this.left < actor.right &&
    this.top < actor.bottom &&
    this.bottom > actor.top){
    return true;
    }
    return false;
  }
}

class Level{
  constructor(grid = [], actors = []){
    this.grid = grid; // сетка игрового поля
    this.actors = actors; // список движущихся объектов игрового поля
    this.height = (this.grid === undefined) ? 0 : this.grid.length; // высота игрового поля, равное числу строк в сетке из первого аргумента.
    this.width = (this.grid === undefined) ? 0 : this.grid.reduce(function(a,b){ // ширина игрового поля, равное числу ячеек в строке сетки из первого аргумента.
      return b.length > a ? b.length : a;
    },0);
    this.status = null; // состояние прохождения уровня
    this.finishDelay = 1; // таймаут после окончания игры
    this.player = this.actors.find(act =>{ // движущийся объект
      return act.type === 'player';
    });
  }
  isFinished(){ // определяет, завершен ли уровень
    if(this.status !== null && this.finishDelay < 0){
      return true;
    }
    return false;
  }
  actorAt(actor){ // расположен ли какой-то другой движущийся объект в переданной позиции
    if(!(actor instanceof Actor) || (!actor)){
      throw new Error(`Не является экземпляром Actor или не передано аргументов`);
    }
    return this.actors.find(act => // если переданный объект пересекается с обЪектом или объектами
      act.isIntersect(actor));
  }
  obstacleAt(pos, size){ // Аналогично методу actorAt определяет, нет ли препятствия в указанном месте. Также этот метод контролирует выход объекта за границы игрового поля.
    if(!(pos instanceof Vector) && !(size instanceof Vector)){
      return new Error(`Не является экземпляром Vector или не передано аргументов`);
    }
    let left = Math.floor(pos.x);
    let right = Math.ceil(pos.x + size.x);
    let top = Math.floor(pos.y);
    let bottom = Math.ceil(pos.y + size.y);


    if(left < 0 || right > this.width || top < 0){
      return 'wall';
    }
    else if(bottom > this.height){
      return 'lava';
    }
    for(let i = top; i < bottom; i++){
      for(let k = left; k < right; k++){
        if(this.grid[i][k]){
          return this.grid[i][k]
        }
      }
    }
  }
  removeActor(actor){ // Метод удаляет переданный объект с игрового поля.
    this.actors = this.actors.filter(function(el){
       return el !== actor;
    });
  }
  noMoreActors(type){ // Определяет, остались ли еще объекты переданного типа на игровом поле.
    return !this.actors.find(el => el.type === type);
  }
  playerTouched(type, actor){ // Меняет состояние игрового поля при касании игроком каких-либо объектов или препятствий.
    if(type === 'lava' || type === 'fireball'){
      this.status = 'lost';
    }
    else if(type === 'coin'){
      this.actors = this.actors.filter(el => el !== actor);
      if(this.noMoreActors(actor)){
        this.status = 'won';
      }
   }
  }
}

class LevelParser{ // позволяет создать игровое поле Level из массива строк
  constructor(map){ // словарь
    this.map = map;
  }
  actorFromSymbol(symbol){ // Возвращает конструктор объекта по его символу, используя словарь. 
    if(symbol === undefined){
      return undefined;
    }
    else{
      return this.map[symbol];
    }
  }
  obstacleFromSymbol(symbol){ // Возвращает строку, соответствующую символу препятствия.
    if(symbol === 'x'){
      return 'wall';
    }
    else if(symbol === '!'){
      return 'lava';
    }
    if(symbol === undefined){
      return undefined;
    }
  }
  createGrid(plan){ // Принимает массив строк и преобразует его в массив массивов, в ячейках которого хранится либо строка, соответствующая препятствию, либо undefined.
    let planArr = plan.map(el => el.split(''));
    for(let k = 0; k < planArr.length; k++){
      planArr[k] = planArr[k].map(elem => this.obstacleFromSymbol(elem));
    }
    return planArr;
  }
  createActors(plan){ // Принимает массив строк и преобразует его в массив движущихся объектов, используя для их создания конструкторы из словаря.
    let actors = [];
    if(this.map){ // словарь
      plan.map((elemY, y) =>{
        [...elemY].map((elemX, x) =>{
          let res;
          if(typeof (this.map[elemX]) === 'function'){
            res = new this.map[elemX](new Vector(x, y));
            if(res instanceof Actor){
              actors.push(res);
            }
          }
        })
      })
    }
    return actors;
  }
  parse(arr){ // Принимает массив строк, создает и возвращает игровое поле, заполненное препятствиями и движущимися объектами, полученными на основе символов и словаря.
    return new Level(this.createGrid(arr), this.createActors(arr));
  }
}
class Fireball extends Actor{ // станет прототипом для движущихся опасностей на игровом поле. Он должен наследовать весь функционал движущегося объекта Actor.
  constructor(pos, speed){
    super(pos, new Vector(1, 1), speed)
  }
  get type(){
    return 'fireball';
  }
  getNextPosition(time = 1){ // Создает и возвращает вектор Vector следующей позиции шаровой молнии.
    return new Vector((time * this.speed.x + this.pos.x), (time * this.speed.y + this.pos.y))
  }
  handleObstacle(){ // Меняет вектор скорости на противоположный.
    this.speed.x *= -1;
    this.speed.y *= -1;
  }
  act(time, level){ // Обновляет состояние движущегося объекта.
    let newPosition = this.getNextPosition(time);
    if(!(level.obstacleAt(newPosition, this.size))){
      this.pos = newPosition;
    }
    else{
      this.handleObstacle();
    }
  }
}
class HorizontalFireball extends Fireball{ // Он будет представлять собой объект, который движется по горизонтали со скоростью 2 и при столкновении с препятствием движется в обратную сторону.
  constructor(pos){
    super(pos, new Vector(2, 0));
  }
}
class VerticalFireball extends Fireball{ // Он будет представлять собой объект, который движется по вертикали со скоростью 2 и при столкновении с препятствием движется в обратную сторону.
  constructor(pos){
    super(pos, new Vector(0,2));
  }
}
class FireRain extends Fireball{ // Он будет представлять собой объект, который движется по вертикали со скоростью 3 и при столкновении с препятствием начинает движение в том же направлении из исходного положения, которое задано при создании.
  constructor(pos = new Vector(0, 0)){
    super(pos, new Vector(0, 3));
    this.position = pos;
  }
  handleObstacle(){
    this.pos = this.position;
  }
}
class Coin extends Actor{ // реализует поведение монетки на игровом поле.
  constructor(pos){
    super(pos);
    Object.defineProperty(this, 'type', {value : 'coin'});
    this.size = new Vector(0.6, 0.6);
    this.pos = this.pos.plus(new Vector(0.2, 0.1));
    this.spring = Math.random() * 2 * Math.PI;
    this.springSpeed = 8;
    this.springDist = 0.07;
    this.position = this.pos;
  }
  updateSpring(time = 1){ // Обновляет текущую фазу spring, увеличив её на скорость springSpeed, умноженную на время.
    this.spring = this.spring + this.springSpeed * time;
  }
  getSpringVector(){ // Создает и возвращает вектор подпрыгивания.
    return new Vector(0, Math.sin(this.spring) * this.springDist);
  }
  getNextPosition(time = 1){ // Создает и возвращает вектор новой позиции монетки.
    this.updateSpring(time);
    return this.position.plus(this.getSpringVector());
  }
  act(time){ // Получает новую позицию объекта и задает её как текущую.
    this.pos = this.getNextPosition(time);
  }
}
class Player extends Actor{ // содержит базовый функционал движущегося объекта, который представляет игрока на игровом поле.
  constructor(pos){
    super(pos, new Vector(0.8, 1.5));
    this.pos = this.pos.plus(new Vector(0, -0.5));
  }
  get type(){
    return 'player';
  }
}

const actorDict = {
 '@': Player,
 'o': Coin,
 'v': FireRain,
 '|': VerticalFireball,
 '=': HorizontalFireball
};

const parser = new LevelParser(actorDict);

loadLevels().then(lev => {
 return runGame(JSON.parse(lev), parser, DOMDisplay)
}).then(result => alert('Вы выиграли!'));