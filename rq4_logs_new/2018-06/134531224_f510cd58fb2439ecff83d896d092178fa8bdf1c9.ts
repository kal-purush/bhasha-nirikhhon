class Normal implements Behaviour {
    triangle: Triangle;

    constructor(triangle: Triangle) {
        this.triangle = triangle
    }
    performBehaviour(): void {
        this.onNormal()
    }
    onSpeedUp(): void {
    }
    onSpeedDown(): void {
    }
    onNormal(): void {
        this.triangle.speed = 5;
    }
}