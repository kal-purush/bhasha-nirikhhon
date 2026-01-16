/// <reference path="../declareFiles/coreLib.d.ts" />

module Sanara.Std {
    export class Text extends Sanara.Core.Fragment {

        private static TEST_CONTEXT = <Sanara.Core.SanaraContext>document.createElement("canvas").getContext("2d");

        private font : string; // SIZEpx FONT_FAMITY_NAME eg. 18px serif

        private myText;

        constructor (children, parameters : Sanara.Core.Value[]) {
            if(parameters.length < 1) {
                throw Sanara.Core.Exception.FEW_PARAMETERS;
            }
            this.font = parameters[0].toString();

            this.myText = "";

            super(null,null,[
                {name:"width", value:this.calcWidth()},
                {name:"height", value:this.calcHeight()}
            ]);
        }

        get text() : string {
            return this.myText;
        }
        set text(newText) {
            this.myText = newText;
            this.setPropertyValue("width",this.calcWidth());
            this.setPropertyValue("height",this.calcHeight());
        }

        private calcHeight() {
            if(this.text.length === 0) return Sanara.Core.Value.ZERO;

            return new Sanara.Core.Value(parseInt(this.font));
        }

        private calcWidth() {
            if(this.text.length === 0) return Sanara.Core.Value.ZERO;

            var w = 0;
            Text.TEST_CONTEXT.save(); {
                Text.TEST_CONTEXT.font = this.font;
                w = Text.TEST_CONTEXT.measureText(this.text).width;
            }; Text.TEST_CONTEXT.restore();
            return new Sanara.Core.Value(w);
        }

        paintImplementation(context : Sanara.Core.SanaraContext) {
            context.font = this.font;
            context.textAlign = 'left'
            context.textBaseline = 'top'
            context.fillText(this.text,0,0);
        }
    }
}