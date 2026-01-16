import * as THREE from "./three.module.js";
import * as paths from "./paths.js";

export function Pathoid(template, path, numChildren, cycleTime) {
    this.template = template;
    this.subpathoids = [];
    this.path = path;
    this.numChildren = 0;
    this.cycleTime = cycleTime;
    this.obj3d = new THREE.Group();
    this.t = 0.0;
    this.onTick = (tDelta) => {
        let u = paths.normalizeRange(0.0, this.cycleTime, this.t);
        for (var i = 0; i < this.numChildren; i++) {
            this.obj3d.children[i].position.copy(path(u));
            u += (1.0 / this.numChildren);
            this.subpathoids[i].onTick(tDelta);
        }
        this.t += tDelta;
        this.t %= this.cycleTime;
    };
    this.clone = () => {
        return new Pathoid(this.template, this.path, this.numChildren, this.cycleTime);
    };
    this.more = (n) => {
        this.numChildren += n;
        for (var i = 0; i < n; i++) {
            let base = this.template.clone();
            this.subpathoids.push(base);
            this.obj3d.add(base.obj3d);
        }
    };
    this.less = (n) => {
        this.numChildren += n;
        for (var i = 0; i < Math.abs(n); i++) {
            let base = this.subpathoids.pop();
            this.obj3d.remove(base.obj3d);
        }
    };

    this.more(numChildren);
}
