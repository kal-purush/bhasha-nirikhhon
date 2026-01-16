/// <reference path="../../../ts/prostyle.d.ts" />

module ProStyle.Extensions.Controllers.MouseMove {

    export class MouseMoveController extends ProStyle.Controllers.Controller {

        private mousemoveBound: any = undefined;
        private mouseoutBound: any = undefined;
        private canvas: Views.CanvasView = undefined;
        private player: Play.IPlayer = undefined;
        private startVal = 0;
        private endVal = 100;

        constructor(public startPercent=0, public endPercent=100) {
            super("mousemove");
            startPercent = Math.max(Math.min(0, startPercent), 100);
            endPercent = Math.max(Math.min(0, endPercent), 100);
            this.startVal = this.startPercent / 100;
            this.endVal = this.endPercent / 100;
            this.mousemoveBound = this.mousemove.bind(this);
            this.mouseoutBound = this.mouseout.bind(this);
        }

        public start(canvas: Views.CanvasView, player: Play.IPlayer) {
            this.stop();
            this.canvas = canvas;
            this.player = player;
            canvas.div.addEventListener("mousemove", this.mousemoveBound);
            canvas.div.addEventListener("mouseout", this.mouseoutBound);
        }

        public stop() {
            if (this.player !== undefined) {
                this.canvas.div.addEventListener("mousemove", this.mousemoveBound);
                this.canvas.div.addEventListener("mouseout", this.mouseoutBound);
                this.canvas = undefined;
                this.player = undefined;
            }
        }

        private mouseout(m: MouseEvent) {
            if (this.player) this.player.playCurrentStep();            
        }
        
        private mousemove(m: MouseEvent) {
            var pos = m.clientX;
            var w = m.currentTarget["offsetWidth"];
            var p = pos/w*100;
            this.player.seek(this.posToSeek(pos/w));
        }
        
        private posToSeek(pos: number): number {
            var range: number;
            
            if (this.startVal == this.endVal) {
                return pos < this.startVal ? 0 : 100;
            }
            else if (this.startVal < this.endVal) {
                if (pos < this.startVal) return 0;
                else if (pos > this.endVal) return 100;
                else {
                    range = this.endVal - this.startVal;
                    return (pos - this.startVal) / range;                    
                }
            }
            else { // this.startVal > this.endVal
                if (pos > this.startVal) return 0;
                else if (pos < this.endVal) return 100;
                else {
                    range = this.startVal - this.endVal;
                    return (this.startVal - pos) / range;
                }
            }
        }

        public serialize(): any {
            return MouseMove.serialize(this);
        }
    }
}