namespace plasma {
    export class XorShift {
        constructor(private seed: number) {
        }

        public run() {
            this.seed ^= (this.seed << 17);
            this.seed ^= (this.seed >> 9);
            this.seed ^= (this.seed << 8);
            this.seed ^= (this.seed >> 27);
            return this.seed;
        }
    }
    export function make_xorshift(seed: number) {
        var engine = new XorShift(seed);
        return () => {
            return engine.run();
        };
    }

    export class CanvasTraits {
        private canvas: HTMLCanvasElement;
        private ctx: CanvasRenderingContext2D;
        public flag: boolean;
        constructor(field_name: string) {
            this.canvas = <HTMLCanvasElement>document.getElementById(field_name);
            if (!this.canvas || !this.canvas.getContext) {
                this.flag = false;
            }
            else {
                this.ctx = this.canvas.getContext('2d');
                this.flag = true;
            }
        }

        public draw_string(
            str: string,
            color: number[],
            size: number,
            offset_x: number,
            offset_y: number
        ) {
            if (!this.flag) return;
            this.ctx.fillStyle = "rgb(" + color[0] + "," + color[1] + "," + color[2] + ")";
            this.ctx.font = size + "px 'ＭＳ Ｐゴシック'";
            this.ctx.fillText(str, offset_x, offset_y);
        }
        public draw_string_style(
            str: string,
            style: string,
            size: number,
            offset_x: number,
            offset_y: number
        ) {
            if (!this.flag) return;
            this.ctx.fillStyle = style;
            this.ctx.font = size + "px 'ＭＳ Ｐゴシック'";
            this.ctx.fillText(str, offset_x, offset_y);
        }

    }
}