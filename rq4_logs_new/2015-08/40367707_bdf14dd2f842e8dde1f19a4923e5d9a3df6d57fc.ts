/// <reference path="../../../ts/prostyle.d.ts" />

module ProStyle.Extensions.Controllers.MouseWheel {

    export class MouseWheelController extends ProStyle.Controllers.Controller {

        private wheelEventBound: any = undefined;
        private canvas: Views.CanvasView = undefined;
        private player: Play.IPlayer = undefined;

        constructor(public speed = 0.01, public ctrlSpeed = 0.001, public altSpeed = 1, public altCtrlSpeed = 0.1) {
            super("mousewheel");
            this.wheelEventBound = this.wheelEvent.bind(this);
        }

        public start(canvas: Views.CanvasView, player: Play.IPlayer) {
            this.stop();
            this.canvas = canvas;
            this.player = player;
            canvas.div.addEventListener("wheel", this.wheelEventBound);
        }

        public stop() {
            if (this.player !== undefined) {
                this.canvas.div.removeEventListener("wheel", this.wheelEventBound);
                this.canvas = undefined;
                this.player = undefined;
            }
        }

        private wheelEvent(e: WheelEvent) {
            var t: number;
            var forward = Util.convertToNumber(e.deltaY, -1) < 0;

            if (e.shiftKey) {
                t = this.moveByStep(forward);
            }
            else {
                var delta = (e.altKey ? (e.ctrlKey ? this.altCtrlSpeed : this.altSpeed) : (e.ctrlKey ? this.ctrlSpeed : this.speed));
                if (!forward) delta = -delta;
                t = this.moveByTime(delta);
            }

            var d = this.player.timeline.totalDuration();
            if (t < 0.01) t = d;
            else if (t > d) t = 0.01;
            this.player.timeline.totalTime(t);
            this.player.timeline.pause(t);
            this.player.pause();
            e.preventDefault();
            return false;
        }

        private moveByStep(forward: boolean): number {
            var step = this.player.getCurrentStep();
            if (forward) {
                if (step.playerStepIndex < this.player.steps.length - 1) {
                    this.player.seekStep(this.player.steps[step.playerStepIndex + 1]);
                }
            }
            else {
                if (step.playerStepIndex > 0) {
                    this.player.seekStep(this.player.steps[step.playerStepIndex - 1]);
                }
            }
            return 0;
        }

        private moveByTime(delta: number): number {
            var t = this.player.timeline.totalTime() + delta;
            t = Math.round(t / delta) * delta;
            return t;
        }

        public resize() {
        }

        public serialize(): any {
            return MouseWheel.serialize(this);
        }
    }
}