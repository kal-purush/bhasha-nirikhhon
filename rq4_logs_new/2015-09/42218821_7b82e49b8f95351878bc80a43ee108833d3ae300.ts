interface Item extends XY{
    render(ctx: CanvasRenderingContext2D);
}
interface Moving extends Item {
    move(ms: number);
    reverse();
}
interface Lifeform {
    birth: number; //date.now
    death?: number;  //date.now
}
interface XY {
    x: number;
    y: number
}

//ideally the plugin system would send the parameters of (movedPerson, closeNeighbors)
    //Can be just change person based on its location,
    //also can be change person to go to closest female.

class World {
    items: Item[] = [];
    size = {x: 720, y: 480}
    renderer = new Renderer("canvas", this.size);
    intervalMs = 50;
    interval = setInterval(this.onTimeSlice.bind(this), this.intervalMs);
    lastRendered = Date.now();

    timesliceListeners = [this.moveAll.bind(this)];

    constructor() {
        for (var i = 0; i < 20; i++)
            this.items.push(<Person> randomize(new Person, this.size))
    }
    moveAll(ms: number){
        var t = Date.now();
        forEach(this.items,Person, (v:Person) => {
            this.movePerson(v, ms);
            if(v.death < t)
                this.items.splice(this.items.indexOf(v), 1) //remove
        });
    }
    movePerson(v:Person, ms: number){
        v.move(ms); //move length of time elapsed since last move
        if(v.x > this.size.x //hit a wall
        || v.y > this.size.y
        || v.x < 0
        || v.y < 0)
            v.direction = Math.random() * 360; //random dir
    }
    onTimeSlice(){
        this.renderer.clearScene();
        var msElapsed = this.lastRendered - Date.now();
        for (let i = 0; i < this.timesliceListeners.length; i++)
            this.timesliceListeners[i](msElapsed);
        this.renderer.render(this.items);
        this.lastRendered = Date.now();
    }
}

enum GENDER { MALE, FEMALE }

function forEach(arr:Item[], c:any, cb:(x) => any){
    for(var i=0;i< arr.length; i++){
        if(arr[i] instanceof c)
            cb(arr[i])
    }
}

function percentChance(a: number, b: () => any) {
    if((Math.random() * 100) < a)
        b()
}

function randomize(a: XY, b: XY) {
    a.x = Math.floor(Math.random() * b.x);
    a.y = Math.floor(Math.random() * b.y);
    if (a instanceof Person) {
        a.birth = Date.now() - Math.floor(Math.random() * 20000)
        a.gender = GENDER.FEMALE;
        percentChance(50, () => a.gender = GENDER.MALE)
        a.speed = Math.random() * 30;
        a.direction = Math.random() * 360;
    }
    return a;
}

class Person implements Moving, Lifeform {
    movePlugins: Array<(p: Person) => any> = [];
    birth = Date.now();
    death = this.birth + Math.floor(Math.random()*50000)
    direction: number; // angle in degrees, 0 == right
    speed: number;     // pixels/second
    x = 0;
    y = 0;
    size = 10; //radius of person.
    get child():boolean{
        return (Date.now() - this.birth) < 10000
    }
    gender: GENDER;
    move(ms: number) {
        //go through and change x and y
        //according to direction and speed.
        var length = this.speed * (ms/1000);
        this.x += Math.cos(this.direction) * length;
        this.y += Math.sin(this.direction) * length;
    }
    reverse() {
        this.direction = (this.direction + 180) % 360;
    }
    get color() {
        if (this.gender === GENDER.MALE)
            return this.child ? "orange" : "blue"
        if (this.gender === GENDER.FEMALE)
            return this.child ? "pink" : "purple"
    }
    render(ctx: CanvasRenderingContext2D) {
        ctx.fillStyle = this.color;
        ctx.beginPath();
        ctx.arc(this.x, this.y, this.size, 0, 2 * Math.PI)
        ctx.fill();
    }
}

class Renderer {
    constructor(id: string, bounds: XY){
        this.canvasId = id;
        this.canvas = <any> document.getElementById(this.canvasId);
        this.canvas.width = bounds.x;
        this.canvas.height = bounds.y;
        this.ctx = this.canvas.getContext("2d");
    }
    canvasId: string;
    canvas: HTMLCanvasElement;
    ctx: CanvasRenderingContext2D;
    render(items: Item[]){
        for(var i = 0; i<items.length; i++)
            items[i].render(this.ctx);
    }
    clearScene(){
        this.canvas.width = this.canvas.width;
    }
}

new World;