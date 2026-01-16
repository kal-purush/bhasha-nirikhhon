/**
 * GameObject
 */
class GameObject {

    public x: number;
    public y: number;
    public width: number;
    public height: number;
    protected div: HTMLElement;

    constructor(parent: HTMLElement, name: string, x: number, y: number, w: number, h: number) {
        this.x = x;
        this.y = y;
        this.width = w;
        this.height = h;

        this.div = document.createElement(name);
        parent.appendChild(this.div);
    }

    public draw(){
        this.div.style.transform = "translate(" + this.x + "px," + this.y + "px)";
    }



}