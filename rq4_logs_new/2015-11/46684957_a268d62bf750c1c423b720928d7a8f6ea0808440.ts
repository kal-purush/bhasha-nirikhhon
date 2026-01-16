class Entity {
    private grix: Grix;

    private x: number;
    private y: number;

    getGrix(): Grix {
        return this.grix;
    }

    getX() {
        return this.x;
    }

    getY() {
        return this.y;
    }

    update() {

    }

    render() {
        this.grix.moveTo(this.x, this.y)
        this.grix.render();
    }
}

class Camera {
    private x = 0;
    private y = 0;

    private entity: Entity;

    constructor(x: number, y: number);
    constructor(entity: Entity);
    constructor();
    constructor(x?: number | Entity, y?: number) {
        if (typeof x == 'number') this.setView(<number>x, y)
        else if (x) this.bindTo(<Entity>x);
    }

    setView(x: number, y: number) {
        this.x = x;
        this.y = y;
    }

    getX(): number {
        return this.x;
    }

    getY(): number {
        return this.y;
    }

    setX(x: number) {
        this.x = x;
    }

    setY(y: number) {
        this.y = y;
    }

    getViewMatrix() {
        return Matrix4.translate(-this.x, -this.y);
    }

    applyViewMatrixTo(shader: Shader) {
        shader.getMatHandler().setViewMatrix(this.getViewMatrix());
    }

    update() {
        this.x = this.entity.getX();
        this.y = this.entity.getY();
        if (this.entity != null) this.applyViewMatrixTo(this.entity.getGrix().getShader())
    }

    bindTo(entity: Entity) {
        this.entity = entity;
    }
}