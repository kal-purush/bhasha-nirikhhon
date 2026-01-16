class CollisionHandler {
    constructor() {
        this.collider = new Collider();
    }

    update() {
        // check collision of lander and landscape
        if (lander.position.y  >= landscape.offset - landscape.variance) {
            let prevPoint = landscape.points[0];
            let hit = false;
            for (let i = 1; i < 20; i++) {
                let lineSegment = new Line(prevPoint, landscape.points[i]);
                lander.getCollisionPoints().forEach(point => {
                    if (this.collider.pointLine(point, lineSegment)) {
                        lander.addCollision(lineSegment);
                        hit = true;
                    }
                });
                prevPoint = landscape.points[i];
            }
            if (!hit) lander.resetCollision();
        }
        
    }
};