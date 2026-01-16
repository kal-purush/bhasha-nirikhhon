/// <reference path="../../constants.ts" />

module objects {
    // LABEL Class ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    export class Label extends createjs.Text {
        constructor(x: number, y: number, labelText: string) {
            super(labelText, constants.LABEL_FONT, constants.LABEL_COLOUR);
            this.regX = this.getMeasuredWidth() * 0.5;
            this.regY = this.getMeasuredHeight() * 0.5;
            this.x = x;
            this.y = y;
        }
    }
}   