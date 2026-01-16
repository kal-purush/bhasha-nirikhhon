class Paddle {
    private width: number;
    private height: number;
    private x: number;

    constructor(width: number, height: number) {
        this.width = width;
        this.height = height;
    }

    public getWidth(): number {
        return this.width;
    }

    public getHeight(): number {
        return this.height;
    }

    public getX(): number {
       return this.x;
    }

    public setX(x: number): void {
        this.x = x;
    }

}