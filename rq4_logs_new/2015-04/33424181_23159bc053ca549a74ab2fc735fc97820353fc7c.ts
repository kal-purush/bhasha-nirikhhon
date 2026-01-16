/// <reference path="../_references.d.ts" />
/// <reference path="../draw/sprites/sprite.ts" />

module Engine.Actors {
    export class BaseActor extends Draw.Sprites.Sprite implements Engine.Physics.IMoves {
        width: number;
        height: number;
        vx: number = 0;
        vy: number = 0;
        dx: number = 0;
        dy: number = 0;
        speed: number;
        jumping: boolean;
        grounded: boolean = false;
        controllable: boolean = false;

        constructor(actorWidth:number, actorHeight: number, actorImage: HTMLImageElement, x: number, y: number, speed: number, controllable: boolean) {
            super(<Draw.Sprites.ISpriteOptions> {
                x: x + (actorWidth / 2),
                y: y + (actorHeight / 2),
                image: actorImage,
                frames: 2,
                startFrame: 0,
                width: actorImage.width,
                height: actorImage.height,
                context: context,
                timePerFrame: FPS
            });

            this.width = actorWidth;
            this.height = actorHeight;
            this.controllable = controllable;
            this.speed = speed;
        }
        
        render() {
            this.update();
            this.draw();
        }
    }
} 