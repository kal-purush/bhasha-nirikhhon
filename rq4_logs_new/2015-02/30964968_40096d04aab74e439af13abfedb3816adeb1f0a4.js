/*!
 * jquery-layout - v1.0
 * Copyright (c) 2013 Aaron Hall
 * Wed, 18 Sep 2013 20:18 GMT
 * Licensed MIT
 */

(function() {

    var fillValidate = [];
    var fillState    = 0;
    var startTime, endTime;

    $.fn.layout = function() {
        startTime = Date.now();
        if (fillState > 0) {
            fillState = 2;
            return;
        }
        fillState = 1;

        var topElem;
        this instanceof jQuery ? topElem = this : topElem = document;

        var self = this;
        var selector = '.fh';
        $(topElem).find(selector).each(function(index) {

            if (this.localName == 'body') {
                $(this).outerHeight($(window).height(), true);
            } else {
                var $parent = $(this).parent();
                if ($parent.hasClass('jql-fh')) {
                    return;
                } else {
                    $parent.addClass('jql-fh');
                }

                var remHeight = $parent.height();
                var vBox      = $parent.hasClass('v-box');
                var hBox      = $parent.hasClass('h-box');
                var wildcardH = [];
                var $this, styleProps, i;

                $parent.children().each(function(index) {
                    $this = $(this);

                    // needs JQuery 1.9
                    styleProps = $this.css( ['position', 'visibility', 'display'] );

                    if (hBox) {
                        if ($this.hasClass('fh')) { $this.outerHeight(remHeight, true); }
                    } else if (vBox) {
                        if (
                            styleProps.position   == 'absolute' ||
                            styleProps.visibility == 'hidden'   ||
                            styleProps.display    == 'none'     ||
                            this.nodeName.toUpperCase() == 'SCRIPT'
                        ) { return; }
                        else {
                            if ($this.hasClass('fh')) { wildcardH.push(this); }
                            else { remHeight -= $this.outerHeight(true); }
                        }
                    } 
                });
                // Process wildcard heights
                if (wildcardH.length > 0) {
                    var itemHeight = Math.round(remHeight / wildcardH.length);
                    var lastHeight = remHeight;
                    for (i = 0; i < wildcardH.length-1; i++) {
                        $(wildcardH[i]).outerHeight(itemHeight, true);
                        lastHeight -= itemHeight;
                    }
                    $(wildcardH[i]).outerHeight(lastHeight, true);
                }
            }
        });

        $('.jql-fh').removeClass('jql-fh');

        var selector = '.fw';
        $(topElem).find(selector).each(function(index) {
            if (this.localName == 'body') {
                $(this).outerWidth($(window).width(), true);
            } else {
                var $parent = $(this).parent();
                if ($parent.hasClass('jql-fw')) {
                    return;
                } else {
                    $parent.addClass('jql-fw');
                }

                var remWidth      = $parent.width();
                var totalChildren = $parent.children().length;
                var vBox          = $parent.hasClass('v-box');
                var hBox          = $parent.hasClass('h-box');
                var wildcardW     = [];
                var $this, styleProps, i, lastElem;

                $parent.children().each(function(index) {
                    $this = $(this);

                    // needs JQuery 1.9
                    styleProps = $this.css( ['position', 'visibility', 'display'] );

                    if (vBox) {
                        $(this).outerWidth(remWidth, true);
                    }
                    else if (hBox) {                    
                        if (
                            styleProps.position   == 'absolute' ||
                            styleProps.visibility == 'hidden'   ||
                            styleProps.display    == 'none'     ||
                            this.nodeName.toUpperCase() == 'SCRIPT'
                        ) { return; }
                        else {
                            if ($(this).hasClass('fw')) { wildcardW.push(this); }
                            else { remWidth -= $(this).outerWidth(true); }
                        }

                        if (index == totalChildren-1) {
                            lastElem = $(this);
                        }
                    }
                });
                // Process wildcard heights
                if (wildcardW.length > 0) {
                    var itemWidth = Math.round(remWidth / wildcardW.length);
                    var lastWidth = remWidth;
                    for (i = 0; i < wildcardW.length-1; i++) {  
                        $(wildcardW[i]).outerWidth(itemWidth, true);
                        lastWidth -= itemWidth;
                    }
                    $(wildcardW[i]).outerWidth(lastWidth, true);
                }

                // Used to auto detect incorrect dimension values as a result
                // of the time required to properly render dom elements.
                var parentEnd = $parent.offset().left + $parent.outerWidth(true),
                    elEnd     = lastElem.offset().left + lastElem.outerWidth(true);

                var validatorObj = {
                    $parent   : $parent,
                    el        : lastElem,
                    locDiff   : parentEnd - elEnd
                };
                fillValidate.push(validatorObj);
                // ----------------------------------------------------------
            }
        });

        $('.jql-fw').removeClass('jql-fw');

        var selector = '.align-middle';
        $(topElem).find(selector).each(function(index) {
            var $self    = $(this);
            var $parent  = $self.parent();
            var elHeight = $self.outerHeight();
            var parentHeight, y, top;

            $parent[0].localName === 'body' ? parentHeight = $(window).height() : parentHeight = $parent.height();

            y = Math.round((parentHeight - elHeight) / 2);
            top = y + 'px';

            if ($self.hasClass('if-it-fits') || $self.hasClass('if-it-fits-height')) {
                y < 0 ? top = '' : '';
            }
            $self.css('margin-top', top);
        });

        // Used to auto-detect incorrect dimension values as a result
        // of the time required to fully render dom elements.
        clearInterval(validateInterval);
        var cnt = 0,
        validateInterval = setInterval(function() {
            var i, len = fillValidate.length, vldObj, diff;
            for ( i = 0; i < len; i++ ) {
                if (cnt++ > 30) clearInterval(validateInterval);
                vldObj = fillValidate[i];
                diff = (vldObj.$parent.offset().left + vldObj.$parent.outerWidth(true)) - (vldObj.el.offset().left + vldObj.el.outerWidth(true));

                if (diff != vldObj.locDiff) {
                    clearInterval(validateInterval);
                    console.log('start new layout'); console.log('diff: '+diff); console.log(vldObj);
                    $.layout();
                    break;
                }
                else {
                    console.log('layout good');
                }
            }
        }, 10);
        // ----------------------------------------------------------

        endTime = Date.now();
        // console.log('layout time: ' + (endTime-startTime));

        // State-based throttle is more efficient than time-based throttle
        if (fillState === 1) {
            fillState = 0;
        } else {
            fillState = 0;
            setTimeout(function() {
                console.log('queued layout request');
                $.layout();
            },0)
        }
        return this;
    }
    $.extend({layout: $.fn.layout});

})();
