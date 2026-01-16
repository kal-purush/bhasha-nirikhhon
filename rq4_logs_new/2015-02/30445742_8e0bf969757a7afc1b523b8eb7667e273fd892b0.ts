/// <reference path="_dependencies.ts" />
module FreeHand {
    "use strict";

    export class PanZoomRotate {
        startDist: number = 0;
        startTransform: Transform = null;
        wasValid: boolean = false;

        construtor() {}

        updateMouseInput(transform: Transform, mouse: MouseInfo, zoom: number): Transform {
            if (mouse.state !== 'wheel') {
                return null;
            }

            var scale = 1;
            if (mouse.dy < 0)
                scale = zoom;
            else if (mouse.dy > 0)
                scale = 1 / zoom;

            if (scale !== 1) {
                transform.setTranslate(
                    mouse.x - (mouse.x - transform.tx) * scale,
                    mouse.y - (mouse.y - transform.ty) * scale);
                transform.scale(scale);
            }
            return transform;
        }

        // updateTouchInput(transform: Transform, touches: TouchInfo[]): Transform {
        //     var one = -1;
        //     var two = -1;
        //     for (var i = 0; i < touches.length; ++i) {
        //         if (touches[i].state !== 'up') {
        //             if (one === -1)
        //                 one = i;
        //             else if (two === -1)
        //                 two = i;
        //             else
        //                 break;
        //         }
        //     }

        //     if (one === -1 || two === -1) {
        //         this.wasValid = false;
        //         return null;
        //     }

        //     var x = touches[one].x - touches[two].x;
        //     var y = touches[one].y - touches[two].y;
        //     var dist = Math.sqrt(x * x + y * y);

        //     var dx = 0;
        //     var dy = 0;
        //     var scale = 1;

        //     if (!this.wasValid) {
        //         // can't have a zero start distance
        //         if (dist < 0.001) {
        //             this.wasValid = false;
        //             return null;
        //         }

        //         this.startTransform.copy(transform);
        //         this.startDist = dist;
        //         this.wasValid = true;
        //     } else {
        //         scale = dist / this.startDist;
        //         x = x / scale - this.startX;
        //         y = y / scale - this.startY;

        //         dx = this.lastX - x;
        //         dy = this.lastY - y;
        //     }
        //     this.lastX = x;
        //     this.lastY = y;

        //     // x,y,dx,dy are in unscaled pixels (i.e. when the pinch was started)
        //     return {
        //         x: x,
        //         y: y,
        //         dx: dx,
        //         dy: dy,
        //         scale: scale
        //     };
        // }
    }
}