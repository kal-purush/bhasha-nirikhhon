const HALF_PI = Math.PI / 2;
const DOUBLE_PI = Math.PI * 2;

export default class Models {
    public pitch = 0;
    public yaw = 0;
    public zoom = 1;

    addPitch(y: number) {
        this.pitch += y - (this.zoom * y * 0.225);
        if (this.pitch < -HALF_PI) {
            this.pitch = -HALF_PI;
        } else if (HALF_PI < this.pitch) {
            this.pitch = HALF_PI;
        }
    }

    addYaw(x: number) {
        this.yaw += x - (this.zoom * x * 0.225);
        if (this.yaw < 0) {
            this.yaw += DOUBLE_PI;
        } else if (DOUBLE_PI < this.yaw) {
            this.yaw -= DOUBLE_PI;
        }
    }

    addZoom(z: number) {
        this.zoom += z;
        if (this.zoom < 0) {
            this.zoom = 0;
        } else if (1 < this.zoom ) {
            this.zoom = 1;
        }
    }
}