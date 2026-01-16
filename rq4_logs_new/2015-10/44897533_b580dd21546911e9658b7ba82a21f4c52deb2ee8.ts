class Brick {
    public static width: number = 75;
    public static height: number = 20;
    public static padding: number = 10;
    public static offsetTop: number = 30;
    public static offsetLeft: number = 30;

    private x: number = 0;
    private y: number = 0;
    private health: number = 100;

    public getX(): number {
        return this.x;
    }

    public setX(x: number): void {
        this.x = x;
    }

    public getY(): number {
        return this.y;
    }

    public setY(y: number): void {
        this.y = y;
    }

    public getHealth(): number {
        return this.health;
    }

    public setHealth(health: number): void {
        this.health = health;
    }

}