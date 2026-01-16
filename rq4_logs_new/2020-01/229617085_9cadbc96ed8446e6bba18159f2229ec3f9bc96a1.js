class Pai {
    visibleNames = [
        "🀇", "🀈", "🀉", "🀊", "🀋", "🀌", "🀍", "🀎", "🀏",
        "🀐", "🀑", "🀒", "🀓", "🀔", "🀕", "🀖", "🀗", "🀘",
        "🀙", "🀚", "🀛", "🀜", "🀝", "🀞", "🀟", "🀠", "🀡",
        "🀀", "🀁", "🀂", "🀃", "🀆", "🀅", "🀄",
        "🀢", "🀣", "🀤", "🀥", "🀦", "🀧", "🀨", "🀩", "🀪", "🀫"];

    constructor(x, y, v) {
        this.pos = new cc.p(x, y);
        this.value = v;
    }

    setWinSize(b, l) {
        var margin = cc.p(6, 2);
        this.winBase = cc.pAdd(b, margin);
        this.winLimit = cc.pSub(l, margin);
        this.winSize = new cc.size(this.winLimit.x - this.winBase.x, this.winLimit.y - this.winBase.y);
    }

    getSprite() {
        var sprite = cc.Layer.create();
        // sprite.setTextureRect(new cc.Rect(this.winBase.width, this.winBase.height, this.winLimit.width, this.winLimit.height));
        // sprite.setColor(cc.color.ORANGE);
        var bg = new cc.DrawNode();
        bg.drawRect(this.winBase, this.winLimit,
            cc.color.WHITE, 1, cc.color.BLACK);
        sprite.addChild(bg, 0);

        var str = cc.LabelTTF.create(`${this.visibleNames[this.value]}`, "Arial", 30);
        let winMid = cc.pAdd(this.winBase, new cc.p(this.winSize.width / 2, this.winSize.height / 2 - 5));
        str.setPosition(winMid.x, winMid.y);
        str.setFontFillColor(cc.color(0, 0, 0, 255));
        sprite.addChild(str, 1);

        return sprite;
    }
}

class Stage {
    constructor(w, h, b, l) {
        this.size = new cc.Size(w, h);
        var margin = cc.p(80, 80);
        this.winBase = cc.pAdd(b, margin);
        this.winLimit = cc.pSub(l, margin);
        this.winSize = new cc.size(this.winLimit.x - this.winBase.x, this.winLimit.y - this.winBase.y);

        this.init();
    }

    init() {
        // 牌種の設定
        let cellNum = this.size.width * this.size.height;
        var paiId = new Array(cellNum);
        for (let i = 0; i < cellNum; i++) {
            paiId[i] = Math.trunc(i / 4);
        }
        for (let i = cellNum - 1; i > 0; i--) {
            var r = Math.trunc(Math.random() * (i + 1));
            var tmp = paiId[i];
            paiId[i] = paiId[r];
            paiId[r] = tmp;
        }

        this.table = new Array(this.size.width);
        for (let x = 0; x < this.size.width; x++) {
            this.table[x] = new Array(this.size.height);
            for (let y = 0; y < this.size.height; y++) {
                this.table[x][y] = new Pai(x, y, paiId[x * this.size.height + y]);
                let pSize = new cc.p(this.winSize.width / this.size.width, this.winSize.height / this.size.height);
                let pBase = cc.pAdd(this.winBase, new cc.p(pSize.x * x, pSize.y * y));
                this.table[x][y].setWinSize(pBase, cc.pAdd(pBase, pSize));
                // cc.lerpで書いたほうがいいかも
            }
        }
    }

    getLayer() {
        var layer = cc.Layer.create();
        var padding = cc.p(80, 80);

        var bg = new cc.DrawNode();
        bg.drawRect(
            cc.pSub(this.winBase, padding), cc.pAdd(this.winLimit, padding),
            new cc.color(0, 85, 46, 255), 1, cc.color.BLACK);
        layer.addChild(bg, 0);

        for (let x = 0; x < this.size.width; x++) {
            for (let y = 0; y < this.size.height; y++) {

                layer.addChild(this.table[x][y].getSprite(), 1);
            }
        }
        return layer;
    }
}