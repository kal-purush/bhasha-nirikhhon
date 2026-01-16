export default class Scale {
    constructor({ key }) {
        this.key = key;
        this.Music = new Vex.Flow.Music();

        this.scale = this.createScale(key);
    }

    get() {
        return this.scale;
    }

    getKey() {
        return this.key;
    }

    createScale(key) {
        const scaleMap = this.createScaleMap(key);
        const keyRoot = key[0].toLowerCase();
        const keyRootIndex = Vex.Flow.Music.roots.indexOf(keyRoot);

        const scale = [];

        for (let i = 0, idx; i < Vex.Flow.Music.roots.length; i++) {
            scale.push(scaleMap[Vex.Flow.Music.roots[(i + keyRootIndex) % Vex.Flow.Music.roots.length]].replace("n", ""))
        }

        return scale;
    }

    createScaleMap(key) {
        return this.Music.createScaleMap(key);    
    }
}

/*Vex.Flow.Music.canonical_notes; // c, c#, d, e, ...
Vex.Flow.Music.scales; // major, dorian, mixo, minor: [2, 1, 2, 2, 1, 2, 2]
const Music = new Vex.Flow.Music();
const scaleTones = Music.getScaleTones(
    Vex.Flow.Music.canonical_notes.indexOf("g"), 
    Vex.Flow.Music.scales.dorian
);
console.log(scaleTones);
console.log(scaleTones.map(s => Music.getCanonicalNoteName(s)));
const scaleMap = Music.createScaleMap("Gm");
console.log(scaleMap);

const scale = [];
const keyRoot = "Gm"[0].toLowerCase();
const keyRootIndex = Vex.Flow.Music.roots.indexOf(keyRoot);
for (let i = 0, idx; i < Vex.Flow.Music.roots.length; i++) {
    scale.push(scaleMap[Vex.Flow.Music.roots[(i + keyRootIndex) % Vex.Flow.Music.roots.length]].replace("n", ""))
}*/