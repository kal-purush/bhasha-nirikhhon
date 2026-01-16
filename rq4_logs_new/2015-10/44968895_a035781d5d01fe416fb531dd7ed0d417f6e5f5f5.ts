/// <reference path="../../../scripts/typings/jquery/jquery.d.ts" />

module Modal {
    "use strict";

    export class Dialog {
        private dialogElementId: string;
        private overlayElementId: string;
        private options: any;

        constructor(dialogElementId: string, overlayElementId: string = "overlay", options: any = {}) {
            this.dialogElementId = dialogElementId;
            this.overlayElementId = overlayElementId;
            this.options = options;
        }

        public Open(): void {
            $(`#${this.dialogElementId}`)
                .css("z-index", 1000)
                .css("position", "absolute")
                .show();

            $(`#${this.overlayElementId}`)
                .css("z-index", 0)
                .css("background-color", this.options.backgroundColor || "#000")
                .css("opacity", this.options.opacity || 0.5)
                .css("position", "absolute")
                .css("top", 0)
                .css("left", 0)
                .css("width", "100%")
                .css("height", $(window).height())
                .show();

            this.Center();
        }

        public Close(): void {
            $(`#${this.dialogElementId}`).hide();
            $(`#${this.overlayElementId}`).hide();
        }

        private Center(): void {
            var top: number,
                left: number,
                $dialogElement = $(`#${this.dialogElementId}`);

            top = Math.max($(window).height() - $dialogElement.outerHeight(), 0) / 2;
            left = Math.max($(window).width() - $dialogElement.outerWidth(), 0) / 2;

            $dialogElement.css({
                top: top + $(window).scrollTop(),
                left: left + $(window).scrollLeft()
            });
        }
    }
} 