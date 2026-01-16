/// <reference path="jquery.d.ts" />
/// <reference path="models.ts" />
module app {
    interface Pt {
        x: number;
        y: number;
    }

    interface Sz {
        w: number;
        h: number;
    }

    interface Rect extends Pt, Sz { }

    interface Canvas extends CanvasRenderingContext2D {
    }

    interface ActiveMsg {
        to: models.Node;
        fr: models.Node;
        msg: any;
    }

    interface NodeLoc extends Pt {
        uid: number;
        tx: number;
        ty: number;
    }

    /**
     * A driver for transition style animations. This triggers callbacks at frame-rate granularity with
     * an adjusted progress parameter.
     * @param callback the callback function for each update.
     * @param duration the full duration of the transition in milliseconds.
     * @param easing an easing function to transform progress before invoking the callback.
     */
    var transition = (callback: (p: number) => void, duration: number, easing?: (p: number) => number) => {
        if (!easing) {
            // If no easing was provided, use linear easing
            easing = (p: number) => { return p; }
        }

        var t0 = Date.now();
        var tick = () => {
            var t1 = Date.now(),
                p = Math.min(1.0, (t1 - t0) / duration);
            callback(easing(p));
            if (p < 1.0) {
                requestAnimationFrame(tick);
            }
        };

        requestAnimationFrame(tick);
    };

    /**
     *
     */
    class WorldView {
        static MODE_IDLE = 0;
        static MODE_SEND = 1;
        static MODE_RECV = 2;

        private locs: NodeLoc[] = [];
        private locsByUid: NodeLoc[] = [];

        // de-normalized model data
        private leader: Pt;
        private msgsSending: ActiveMsg[] = [];
        private nodesRecieving: boolean[] = [];

        // rendering modes
        private mode: number = WorldView.MODE_IDLE;
        private pct: number = 0.0;

        constructor(private title: string,
                    public world: models.World<models.Node>,
                    public canvas: Canvas,
                    public rect: Rect) {

            world.messageWasSent.tap((msg: any, fr: models.Node, to: models.Node) => {
                this.msgsSending.push({
                    to: to,
                    fr: fr,
                    msg: msg,
                });

                this.nodesRecieving[to.uid] = true;
            });

            world.nodeDidBecomeLeader.tap((node: models.Node) => {
                this.leader = this.locsByUid[node.uid];
            });
        }

        /**
         *
         */
        public resize(rect: Rect) {
            var r = Math.min(rect.w, rect.h) / 2 - 60,
                tr = r - 25,
                nodes = this.world.nodes,
                dr = 2 * Math.PI / nodes.length,
                cx = rect.w / 2,
                cy = rect.h / 2,
                locs = this.locs,
                locsByUid = this.locsByUid;

            nodes.forEach((node: models.Node, i: number) => {
                var vx = Math.cos(i * dr),
                    vy = Math.sin(i * dr);
                var pt = {
                    uid: node.uid,
                    x: cx + r * vx,
                    y: cy + r * vy,
                    tx: cx + tr * vx,
                    ty: cy + tr * vy,
                };
                locs[i] = pt;
                locsByUid[node.uid] = pt;
            });
        }

        /**
         *
         */
        public draw() {
            var canvas = this.canvas,
                rect = this.rect;
            canvas.save();
            canvas.beginPath();
            canvas.moveTo(rect.x, rect.y);
            canvas.lineTo(rect.x + rect.w, rect.y);
            canvas.lineTo(rect.x + rect.w, rect.y + rect.h);
            canvas.lineTo(rect.x, rect.y + rect.h);
            canvas.closePath();
            canvas.clip();
            canvas.translate(rect.x, rect.y);
            this.render(canvas, rect);
            canvas.restore();
        }

        /**
         *
         */
        private render(canvas: Canvas, size: Sz) {
            var mode = this.mode,
                locs = this.locs,
                nodes = this.world.nodes,
                first = locs[0],
                n = locs.length,
                locsByUid = this.locsByUid,
                nodesReceiving = this.nodesRecieving,
                msgs = this.msgsSending,
                pct = this.pct;

            canvas.clearRect(0, 0, size.w, size.h);

            // render edges
            canvas.strokeStyle = '#eee';
            canvas.lineWidth = 2;
            canvas.beginPath();
            canvas.moveTo(first.x, first.y);
            for (var i = 1; i < n; i++) {
                canvas.lineTo(locs[i].x, locs[i].y);
            }
            canvas.closePath();
            canvas.stroke();

            // render messages
            if (mode == WorldView.MODE_SEND) {
                msgs.forEach((msg: ActiveMsg) => {
                    var frPt = locsByUid[msg.fr.uid],
                        toPt = locsByUid[msg.to.uid],
                        x = frPt.x + (toPt.x - frPt.x) * pct,
                        y = frPt.y + (toPt.y - frPt.y) * pct;
                    canvas.fillStyle = '#999';
                    canvas.strokeStyle = '#777';
                    canvas.beginPath();
                    canvas.arc(x, y, 6, 0, 2 * Math.PI, false);
                    canvas.fill();
                    canvas.stroke();
                });
            }

            var rect = this.rect,
                cx = rect.w / 2,
                cy = rect.y / 2;

            // render nodes
            canvas.strokeStyle = '#666';
            locs.forEach((loc: NodeLoc, i: number) => {
                var r = (mode == WorldView.MODE_RECV && nodesReceiving[nodes[i].uid])
                    ? 10 + 1.5 * (1 + Math.cos((2 * pct - 1) * Math.PI))
                    : 10;
                canvas.fillStyle = (this.leader == loc) ? '#f90' : '#09f';
                canvas.beginPath();
                canvas.arc(loc.x, loc.y, r, 0, 2*Math.PI, false);
                canvas.fill();
                canvas.stroke();

                var txt = '' + loc.uid,
                    met = canvas.measureText(txt);
                canvas.font = '14px Helvetica';
                canvas.fillStyle = '#999';
                canvas.fillText(txt, loc.tx - met.width/2, loc.ty);
            });

            // render title
            var title = this.title,
                tw = canvas.measureText(title).width;
            canvas.font = '16px Helvetica';
            canvas.fillStyle = '#999';
            canvas.fillText(
                this.title,
                rect.w/2 - tw/2,
                rect.h - 25);
        }

        /**
         * Enter the sending mdoe of the animation where we show messages
         * traversing links to their target nodes.
         */
        private startSendingMessages() {
            this.mode = WorldView.MODE_SEND;
            transition((pct: number) => {
                this.pct = pct;
                this.draw();

                if (pct >= 1.0) {
                    this.startReceivingMessages();
                }
            }, 1000);
        }

        /**
         * Enter the receiving mode of the animation where nodes "absorb"
         * their arriving nodes.
         */
        private startReceivingMessages() {
            this.mode = WorldView.MODE_RECV;
            transition((pct: number) => {
                this.pct = pct;
                this.draw();

                if (pct >= 1.0) {
                    this.start();
                }
            }, 150);
        }

        /**
         * Begin a cycle of the animation loop.
         */
        start() {
            this.mode = WorldView.MODE_IDLE;
            this.msgsSending = [];
            this.nodesRecieving = [];

            this.world.update();

            if (this.msgsSending.length == 0) {
                this.draw();
                return;
            }

            this.startSendingMessages();
        }
    }

    var $e = (name: string) => {
        return $(document.createElement(name));
    };

    var CreateCanvas = (root : JQuery) : Canvas => {
        var ww = window.innerWidth,
            wh = window.innerHeight,
            sz = Math.min(ww, wh);
        var canvas = <HTMLCanvasElement>$e('canvas').addClass('canvas')
            .attr('width', sz)
            .attr('height', sz)
            .css('left', (ww - sz) / 2)
            .css('top', (wh - sz) / 2)
            .appendTo(root).get(0);
        return canvas.getContext('2d');
    };

    var Main = () => {
        var body = $(document.body),
            canvas = CreateCanvas(body),
            w = canvas.canvas.width,
            h = canvas.canvas.height;

        var views = [
            new WorldView(
                "Le Lann, Chang, and Roberts",
                models.lcr.New(10),
                canvas,
                {x: 0, y: 0, w: w/2, h: h/2}),

            new WorldView(
                "Hirschberg and Sinclair",
                models.hs.New(10),
                canvas,
                {x: w/2, y: 0, w: w/2, h: h/2}),

            new WorldView(
                "Idle",
                models.bogus.New(10),
                canvas,
                {x: 0, y: h/2, w: w/2, h: h/2}),

            new WorldView(
                "Idle",
                models.bogus.New(10),
                canvas,
                {x: w/2, y: h/2, w: w/2, h: h/2}),
        ];

        views.forEach((w: WorldView) => {
            w.resize(w.rect);
            w.draw();
            w.start();
        });
    };

    Main();
}