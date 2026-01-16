module X3Map {
    export enum AsteroidType {
        ORE,
        SILICON,
        ICE,
        NVIDIUM
    }

    export class Asteroid extends MapObject3D {
        private d_type: AsteroidType;
        private d_content: number;

        constructor(name: string, type: AsteroidType, posx: number, posy: number, posz: number, content?: number) {
            super(name, MapObjectType.ASTEROID, posx, posy, posz);

            this.d_type = type;
            this.d_content = (content != null) ? content : 0;
        }

        public get Type(): AsteroidType {
            return this.d_type;
        }
    }
}