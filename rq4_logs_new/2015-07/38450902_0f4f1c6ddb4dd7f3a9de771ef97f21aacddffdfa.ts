/// <reference path="typings/tsd.d.ts" />

interface tooltip_options {
    mode?: Tooltip.Mode;
    content?: Tooltip.Content;
    cssClass?: string;
    showOn?: Tooltip.ShowOn;
    closeSelector?: string;
    distance?: number;
}

interface JQuery {
    tooltip(options?: tooltip_options): JQuery;
}

module Tooltip {
    export var activeTooltip: Tooltip;
    export enum Mode { bottom, top }
    export enum Content { title, href }
    export enum ShowOn { hover, click }

    function close() {
        if (activeTooltip)
            activeTooltip.close();
    }

    //Indicates if a jquery set contains a given DOM node
    function $contains($set: JQuery, elem: HTMLElement, includeSelf = true) {
        var e: JQuery = this;
        for (var i = 0; i < e.length; i++) {
            if ((includeSelf && e.get(i) == elem) || $.contains(e.get(i), elem))
                return true;
        }
        return false;
    };

    (function ($) {
        $.fn.tooltip = function (options) {

            options = $.extend({
                mode: Mode.bottom,
                content: Content.title,
                cssClass: '',
                showOn: ShowOn.hover,
                closeSelector: '.tooltip-close',
                distance: 5,
            }, options);

            return this.each(function () {
                var t = new Tooltip(this, options);
            });
        };

        $('html').click(function (ev) {
            //Hide tooltip on click outside target element and popup
            var $active = activeTooltip.tooltip.add(activeTooltip.target);
            if (activeTooltip && !$contains($active, ev.target))
                activeTooltip.close();
        });

        $(window).resize(() => {
            if (activeTooltip)
                activeTooltip.position();
        });
    })(jQuery);

    export class Tooltip {
        private content: JQuery;
        public tooltip: JQuery;
        public target: JQuery;

        constructor(private targetElem: HTMLElement, private options: tooltip_options) {
            targetElem['tooltip'] = this;
            this.target = $(targetElem).addClass('has-tooltip');
            var that = this;

            if (options.showOn == ShowOn.hover) {
                //Reaffect target to fix bug on cloned elements.
                var show = function () { that.target = $(this); that.show(); }

                this.target.hover(
                    show,
                    () => this.close()
                    ).click(show);
            } else {
                this.target.click(function () { that.target = $(this); that.toggle(); return false;});
            }
        }

        private initContent() {
            if (this.content)
                return;
            if (this.options.content == Content.title) {
                var title = this.target.attr('title');
                if (!title)
                    return;
                this.target
                    .attr('data-title', title)
                    .attr('title', '');
                this.content = $('<span/>').html(title);
            } else {
                this.content = $(this.target.attr('href'));
                if (!this.content.length)
                    this.content = null;
            }

            if (this.content)
                this.content.find(this.options.closeSelector).click(() => { this.close(); return false; });
        }

        public toggle() {
            if (this.tooltip)
                this.close();
            else
                this.show();
        }

        public show() {
            this.initContent();
            if (!this.content)
                return;

            if (activeTooltip)
                activeTooltip.close();

            var e = this.target;
            var o = this.options;

            var t = this.tooltip = $('<div class="tooltip-frame"/>')
                .addClass(o.cssClass)
                .addClass('tooltip-' + Mode[o.mode])
                .append(this.content.show())
                .append($('<div class="tip"/>'))
                .appendTo('body');

            activeTooltip = this;
            this.position();
        }

        public position() {
            var margin = 10;

            var t = this.tooltip;
            var e = this.target;
            var o = this.options;

            if (!t)
                return;

            //Reset so dimentions calculations are correct
            t.removeAttr('style');

            var offset = e.offset();

            var ww = $(window).width();
            var w = t.outerWidth();
            var left = offset.left + e.outerWidth() / 2 - w / 2;

            if (left < margin)
                left = margin;
            var rightOverflow = (left + w) - (ww - margin);
            if(rightOverflow > 0)
                left = Math.max(left - rightOverflow, margin);

            t.css({
                'left': left + 'px',
                'max-width': (ww - left - margin) + 'px'
            }).find('.tip')
                .css('left', offset.left + e.outerWidth() / 2 - left + 'px');

            //Setting width can make height vary. So we set vertical position after.
            var h = t.outerHeight();
            t.css('top', o.mode == Mode.top
                ? offset.top - h - o.distance
                : offset.top + e.outerHeight() + o.distance
            );
        }

        public close() {
            if (!this.tooltip)
                return;
            this.content.hide().appendTo('body');
            this.tooltip.remove();
            this.tooltip = null;
            activeTooltip = null;
        }
    }
}