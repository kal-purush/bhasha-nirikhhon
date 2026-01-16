/// <reference path="../../_references.d.ts" />
module Draw.Sprites {
    export class Sprite {
        x: number;
        y: number;

        drawContext: CanvasRenderingContext2D;
        width: number;
        height: number;
        image: HTMLImageElement;
        frameCount: number;
        frameIndex: number;
        timePerFrame: number;
        time: number = 0;

        constructor(options: ISpriteOptions) {
            this.x = options.x;
            this.y = options.y;
            this.drawContext = options.context;
            this.width = options.width + 1;
            this.height = options.height;
            this.image = options.image;
            this.frameCount = options.frames;
            this.frameIndex = options.startFrame || 0;
            this.timePerFrame = options.timePerFrame;
        }

        draw(): void {
            var frameOffsetX: number = this.frameIndex * this.width / this.frameCount,
                frameWidth: number = this.width / this.frameCount;

            this.drawContext.drawImage(this.image, frameOffsetX, 0, frameWidth, this.height, this.x, this.y, frameWidth, this.height);
        }

        update(): void {
            this.time += 1;

            if (this.time > this.timePerFrame) {
                this.time = 0;

                if (this.frameIndex < this.frameCount - 1) {
                    this.frameIndex++;
                } else {
                    this.frameIndex = 0;
                }
            }
        }
    }
}