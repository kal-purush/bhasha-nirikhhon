class Vector2 {

    X: number;
    Y: number;

    constructor(x = 0, y = 0) {
        this.X = x;
        this.Y = y;
    }

    Distance(other: Vector2) {
        var x = other.X - this.X;
        var y = other.Y - this.Y;

        return Math.sqrt(Math.pow(x, 2) + Math.pow(y, 2));
    }

    DistanceSquared(other: Vector2) {
        var x = other.X - this.X;
        var y = other.Y - this.Y;

        return Math.pow(x, 2) + Math.pow(y, 2);
    }

    Magnitude() {
        return Math.sqrt(Math.pow(this.X, 2) + Math.pow(this.Y, 2));
    }

    MagnitudeSquared() {
        return Math.pow(this.X, 2) + Math.pow(this.Y, 2);
    }

    Normalize() {
        var mag = this.Magnitude();
        this.X = this.X / mag;
        this.Y = this.Y / mag;
    }

    Normalized() {
        var newVec = new Vector2(this.X, this.Y);
        newVec.Normalize();

        return newVec;
    }

    Add(other: Vector2) {
        this.X += other.X;
        this.Y += other.Y;
    }

    Added(other: Vector2) {
        return new Vector2(this.X + other.X, this.Y + other.Y);
    }

    Subtract(other: Vector2) {
        this.X -= other.X;
        this.Y -= other.Y;
    }

    Subtracted(other: Vector2) {
        return new Vector2(this.X - other.X, this.Y - other.Y);
    }

    Scale(value: number) {
        this.X *= value;
        this.Y *= value;
    }   

    Scaled(value: number) {
        return new Vector2(this.X * value, this.Y * value);
    }
}