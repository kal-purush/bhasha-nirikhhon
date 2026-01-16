/// <reference path="_dependencies.ts" />
module FreeHand {
    "use strict";

    export interface MouseInfo {
        x: number;
        y: number;
        dx: number;
        dy: number;
        buttonDown: boolean;
    }

    export class MouseInput {
        private mouseDownHandler = this.onMouseDown.bind(this);
        private mouseUpHandler = this.onMouseUp.bind(this);
        private mouseMoveHandler = this.onMouseMove.bind(this);
        private lastX = 0;
        private lastY = 0;
        private isButtonDown = false;

        constructor(private elem: HTMLElement, private onMouseFunc: (info: MouseInfo) => void) {
            elem.addEventListener('mousedown', this.mouseDownHandler);
        }

        onMouseDown(e: MouseEvent) {
            e.preventDefault();

            document.addEventListener("mousemove", this.mouseMoveHandler);
            document.addEventListener("mouseup", this.mouseUpHandler);

            var x = e.pageX - this.elem.offsetLeft;
            var y = e.pageY - this.elem.offsetTop;
            var info: MouseInfo = {
                x: x,
                y: y,
                dx: 0,
                dy: 0,
                buttonDown: true
            };
            this.onMouseFunc.call(this, info);

            this.lastX = x;
            this.lastY = y;
            this.isButtonDown = true;
        }

        onMouseMove(e: MouseEvent) {
            e.preventDefault();

            var x = e.pageX - this.elem.offsetLeft;
            var y = e.pageY - this.elem.offsetTop;
            var info: MouseInfo = {
                x: x,
                y: y,
                dx: x - this.lastX,
                dy: y - this.lastY,
                buttonDown: this.isButtonDown
            };
            this.onMouseFunc.call(this, info);

            this.lastX = x;
            this.lastY = y;
        }

        onMouseUp(e: MouseEvent) {
            e.preventDefault();
            document.removeEventListener("mouseup", this.mouseUpHandler);
            document.removeEventListener("mousemove", this.mouseMoveHandler);

            var x = e.pageX - this.elem.offsetLeft;
            var y = e.pageY - this.elem.offsetTop;
            var info: MouseInfo = {
                x: x,
                y: y,
                dx: x - this.lastX,
                dy: y - this.lastY,
                buttonDown: false
            };
            this.onMouseFunc.call(this, info);

            this.lastX = x;
            this.lastY = y;
            this.isButtonDown = false;
        }
    }

    export interface TouchInfo {

    }

    export class TouchInput {
        private touchStartHandler = this.onTouchStart.bind(this);
        private touchMoveHandler = this.onTouchMove.bind(this);
        private touchEndHandler = this.onTouchEnd.bind(this);

        private lastX = 0;
        private lastY = 0;

        constructor(private elem: HTMLElement, private onTouchFunc: (info: TouchInfo) => void) {
            elem.addEventListener('touchstart', this.touchStartHandler);
        }
    }
}