abstract class GameObject {

    // Props
    protected div: HTMLElement;
    protected x!: number;
    protected y!: number;
    protected width!: number;
    protected height!: number;

    constructor(element: string, parent: HTMLElement, x: number, y: number, height: number, width: number) {

        // Create element and append it to parent
        this.div = document.createElement(element);
        parent.appendChild(this.div);

        // Set position
        this.setX(x);
        this.setY(y);

        // Set sizes
        this.setWidth(width);
        this.setHeight(height);
    }

    public move(): void {
        this.draw();
    }

    // Draw
    public draw(): void {
        this.div.style.transform = "translate(" + this.getX() + "px," + this.getY() + "px)";
    }

    // Getters and setters
    public getX(): number {
        return this.x;
    }

    public setX(xPos: number): void {
        this.x = xPos;
    }

    public getY(): number {
        return this.y;
    }

    public setY(yPos: number): void {
        this.y = yPos;
    }

    public getHeight(): number {
        return this.height;
    }

    public setHeight(height: number): void {
        this.height = height;
    }

    public getWidth(): number {
        return this.width;
    }

    public setWidth(width: number): void {
        this.width = width;
    }

    // Remove object div
    public removeDiv(): void {
        this.div.remove();
    }

}