var transitionEndTrigger = 'transitionend webkitTransitionEnd oTransitionEnd MSTransitionEnd';

$(window).ready(function() {
    F_Global.init();
}).scroll(function() {
}).resize(function() {

});

/**
* Description for GO_Global
* @private
* @property GO_Global
*/
var F_Global = {
    vars: {
    },
    /**
    * Description for init
    * @private
    * @method init
    * @return {Object} description
    */
    init: function() {

    },
    /**
    * Description for is_mobile
    * @private
    * @method is_mobile
    * @return {Object} description
    */
    is_mobile: function() {
        if(NSINKA_Global.vars.is_mobile != null) {
            return NSINKA_Global.vars.is_mobile;
        }

        var check = false;
        (function(a){if(/(android|bb\d+|meego).+mobile|avantgo|bada\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od|ad)|iris|kindle|lge |maemo|midp|mmp|mobile.+firefox|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\.(browser|link)|vodafone|wap|windows ce|xda|xiino/i.test(a)||/1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\-(n|u)|c55\/|capi|ccwa|cdm\-|cell|chtm|cldc|cmd\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\-s|devi|dica|dmob|do(c|p)o|ds(12|\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\-|_)|g1 u|g560|gene|gf\-5|g\-mo|go(\.w|od)|gr(ad|un)|haie|hcit|hd\-(m|p|t)|hei\-|hi(pt|ta)|hp( i|ip)|hs\-c|ht(c(\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\-(20|go|ma)|i230|iac( |\-|\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\/)|klon|kpt |kwc\-|kyo(c|k)|le(no|xi)|lg( g|\/(k|l|u)|50|54|\-[a-w])|libw|lynx|m1\-w|m3ga|m50\/|ma(te|ui|xo)|mc(01|21|ca)|m\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\-2|po(ck|rt|se)|prox|psio|pt\-g|qa\-a|qc(07|12|21|32|60|\-[2-7]|i\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\-|oo|p\-)|sdk\/|se(c(\-|0|1)|47|mc|nd|ri)|sgh\-|shar|sie(\-|m)|sk\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\-|v\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\-|tdg\-|tel(i|m)|tim\-|t\-mo|to(pl|sh)|ts(70|m\-|m3|m5)|tx\-9|up(\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\-|your|zeto|zte\-/i.test(a.substr(0,4))) check = true;})(navigator.userAgent||navigator.vendor||window.opera);
        NSINKA_Global.vars.is_mobile = check;
        return check;
    },
    is_tablet: function() {
        var wW = $(window).width(),
            wH = $(window).height(),
            factor = wW>wH?wW/wH:wH/wW;
        return (
            this.is_mobile() &&
            //$(window).width() > 414 &&
            factor < 1.6
        );
    },
    is_smartphone: function() {
        var wW = $(window).width(),
            wH = $(window).height(),
            factor = wW>wH?wW/wH:wH/wW;
        return (
            this.is_mobile() &&
            //$(window).width() > 414 &&
            factor > 1.6
        );
    },
    ajaxPage: function(url, fnAfter) {
        if(NSINKA_Global.vars.xhr) {
            NSINKA_Global.vars.xhr.abort();
        }

        NSINKA_Global.vars.xhr = $.ajax({
            url : url,
            success: function(data) {
                $data = $.parseHTML(data).filter(function(item) {
                    if(item.nodeName == 'TITLE' || item.nodeName == 'MAIN') {
                        return item;
                    } else {
                        return;
                    }
                });

                title = $data.filter(function(item) {
                    if(item.nodeName == 'TITLE') {
                        return item;
                    } else {
                        return;
                    }
                });

                $('title').text($(title[0]).text());

                fnAfter($data);
            },
            dataType: "html",
        });
    },
    /**
    * Description for tmpl
    * @private
    * @method tmpl
    * @param {Object} id (required)
    * @param {Object} data (default: null)
    * @return {Object} jQuery elements
    */
    tmpl: function(id, data) {
        data = { data : data } || { data : {} };

        return _.template($('#tmpl_'+id).clone().text())(data);
    }
}

/**
* Description for NSINKA_Actions
* @private
* @property NSINKA_Actions
*/
var NSINKA_Actions = {
    vars: {
    },
    init: function($body) {
        $body = $body || $('body');
        $('*[data-jsaction]', $body).add($body.filter('[data-jsaction]')).each(this.__triggerAction);
    },
    __triggerAction: function() {
        var $this = $(this),
            jsaction = $this.data('jsaction');

        $.each(jsaction.match(/\w+(\([^\)]+\))?/g), function(i, fn) {
            var attrs = fn.replace(/^\w+\(|\)$/g, '').split(','),
                fn = fn.match(/^\w+/)[0];
            if(typeof NSINKA_Actions.functions[fn] != 'undefined') {
                NSINKA_Actions.functions[fn]($this, attrs);
            } else {
                console.error('not found! function NSINKA_Actions.functions.'+fn+'('+attrs.join(',')+')');
            }
        });
    },
    functions: {
        openmenu: function($this, attrs) {
            $this.clicktouch(function(e) {
                e.preventDefault();
                e.stopImmediatePropagation();
                var $html = $('html'),
                    opened_class = 'nav_open',
                    button_active_class = 'is-active';
                if($html.hasClass(opened_class)) {
                    $this.removeClass(button_active_class);
                    $html.removeClass(opened_class);
                } else {
                    $this.addClass(button_active_class);
                    $html.addClass(opened_class);
                }
            });
        },
        transformlogo: function($this) {
            $this.on('moveTransitionStatus', function(e, percentage) {
                if(percentage>0) {
                    $this.closest('layer').removeClass('transformed').find('#logo__container span')/*.fadeIn()*/;
                } else {
                    $this.closest('layer').addClass('transformed').find('#logo__container span')/*.fadeOut(function() {
                        //$(this).hide();
                    })*/;
                }
            });
        },
        cloneHeader: function($this) {
            $(window).on('scroll__header', function() {
                if($(window).scrollTop()>2) {
                    $this.trigger('closeLayer');
                }
            });

            //var $logo = $this.find('#logo__container');
            //$logo.after($logo.clone(true,true).addClass('clone'));
        },
        startDirections: function($this) {
            $this.clicktouch(NSINKA_Map.startDirections);
        },
        receiveRouteDuration: function($this) {
            $(window).on('routeHasBeenCalculated', function(e, route) {
                calcDuration(route);
                showWaypoints(route);
            });

            function calcDuration(route) {
                var duration = 0;

                if(!Object.keys(route).length) {
                    return;
                }

                $.each(route.routes[0].legs, function(i, waypoint) {
                    duration += waypoint.duration.value;
                });

                $this.text((Math.round((duration/60/60)*4)/4)+' Std.');

            }

            function showWaypoints(route) {
                var $tmpl = $(NSINKA_Global.tmpl('routeSettings', {
                    directions: NSINKA_Map.vars.waypoints,
                    routes: NSINKA_Map.vars.routes,
                    currentRoute: NSINKA_Map.vars.currentRoute,
                }));

                $this.closest('layer-content').find('layer-content-opened').html('').append($tmpl);

                $tmpl.find('li').each(function(i) {
                    this.mapData = NSINKA_Map.vars.waypoints[i];
                });

                $tmpl.find('ul').sortable({
                    placeholder: "placeholder",
                    stop: function(event,ui){
                        NSINKA_Map.vars.waypoints = [];
                        $tmpl.find('li').each(function() {
                            NSINKA_Map.vars.waypoints.push(this.mapData);
                        });
                        NSINKA_Map.initCustomRoute();
                        NSINKA_Map.genRoute();
                    },
                    axis: "y",
                    helper: 'clone',
                    handle: '.drag-handle'
                });

                NSINKA_Actions.init($tmpl);
            }
        },
        routeSettings: function($this) {
            $this.find('a.deactivate').click(function() {
                var deactivatedClass = 'deactivated',
                    $this = $(this),
                    $row = $this.closest('li');


                if($row.hasClass(deactivatedClass)) {
                    var inactive = false;
                    $row.removeClass(deactivatedClass);
                } else {
                    var inactive = true;
                    $row.addClass(deactivatedClass);
                }

                $changeIcon = $this.data('change-icon');
                $this.data('change-icon', $this.text());
                $this.text($changeIcon);

                NSINKA_Map.vars.waypoints[$row.data('location-id')].inactive = inactive;

                NSINKA_Map.initCustomRoute();
                NSINKA_Map.genRoute();

            })
        },
        search: function($this) {
            $this.find('button').clicktouch(function(e) {
                e.stopImmediatePropagation();
                e.preventDefault();
                var $form = $(this).closest('form'),
                    $input = $form.find('input[type="search"]'),
                    activeClass = 'active';
                if($form.hasClass(activeClass)) {
                    $form.removeClass(activeClass);
                    $input.blur();
                } else {
                    $form.addClass(activeClass);
                    $input.focus();
                }
            });
        },
        openLocation: function($this) {

            $this.click(function() {
                var $tmpl = $(NSINKA_Global.tmpl('location', {directions : NSINKA_Map.vars.waypoints}));

                $('body').prepend($tmpl);

                var $locations__wrapper = $tmpl.find('#locations__wrapper');

                $locations__wrapper.slick({
                    arrows: false,
                    variableWidth: true,
                    initialSlide: $this.closest('li').data('location-id')
                });

                //$tmpl.find('#location_close').slick('slickPrev');
                $tmpl.find('#location_prev,#location_next').click(function() {
                    if(this.id == 'location_next') {
                        $locations__wrapper.slick('slickNext');
                    } else {
                        $locations__wrapper.slick('slickPrev');
                    }
                });

                $tmpl.find('#location_close').click(function() {
                    $tmpl.removeClass('active');
                    $tmpl.one(transitionEndTrigger, function() {
                        $tmpl.remove();
                    });
                })

                setTimeout(function() {
                    $tmpl.addClass('active');
                },100);
            });
        },
        selectRoute: function($this) {
            $this.change(function() {
                NSINKA_Map.initRoute($(this).val());
                NSINKA_Map.genRoute();
            });
        },
        quicklinks: function($this) {
            var $header = $this.find('layer-content-header'),
                headerStandardValue = $header.text();
            $this.on('moveTransitionStatus', function(e, percentage) {
                if(percentage>0) {
                    $header.text(headerStandardValue);
                } else {
                    $header.text('Stadtrundgang');
                }
            });
        }

    }
}

/**
* Description for NSINKA_Actions
* @private
* @property NSINKA_Actions
*/
var NSINKA_jQueryExtensions = {
    vars: {
    },
    init: function() {
        jQuery.fn.extend(this.functions);
    },
    functions: {
        clicktouch: function (fn) {
            return $(this).on('click',function() {
                $(this).trigger('touchstart',[true]);
            }).on('mousedown mousemove mouseup', function(e) {
                e.preventDefault();
                e.stopImmediatePropagation();
                e.stopPropagation();
            }).on('touchstart', fn);
        }

    }
}

/**
* Description for NSINKA_Layers
* @private
* @property NSINKA_Layers
*/
var NSINKA_Layers = {
    vars: {
        snapPoint: 50
    },
    init: function() {
        this.initHtmlLayerTag();
        $('layer').each(this.single);
        this.layerLinks();
        this.identifyLayers();

        /* Safari iOS browserhack inherit min-height*/
        var $content = $('main>.content');
        new ResizeSensor($content[0], function() {
            $content.trigger('browserhack_inherit_height');
        });

        var $article = $('layer>article');
        $('layer>article').each(function() {
            var $this = $(this);
            new ResizeSensor($this.parent()[0], function() {
                $this.trigger('_resize');
            });

            $this.on('_resize', function() {
                $this.css({ 'min-height' : $(window).height() - $this.offset().top });
            }).trigger('_resize');
        });

        $content.on('browserhack_inherit_height', function() {
            $(this).css('height', '').css('height', this.clientHeight);
        }).trigger('browserhack_inherit_height');

    },
    layerLinks: function($body) {
        $body = $body || $('body');
        $('a[href]').click(function(e) { e.preventDefault(); }).clicktouch(function(e) {
            e.preventDefault();
            NSINKA_Global.ajaxPage(this.href, NSINKA_Layers.prepareReceiverLayer);
        });
    },
    identifyLayers: function($body) {
        $body = $body || $('body');
        $('layer', $body).each(function() {
            this.dataset.id = $.map($(this).parents().filter('main,layer'), function(item) {
                return item.nodeName+(item.id?'#'+item.id:'');
            }).reverse().join('>')+'>'+this.nodeName+(this.id?'#'+this.id:'');
        });
    },
    prepareReceiverLayer: function($data) {
        $main = $($data.filter(function(item) {
            if(item.nodeName == 'MAIN') {
                return item;
            } else {
                return;
            }
        })[0]);

        NSINKA_Layers.identifyLayers($main);

        $('layer').each(function() {
            var $t = $(this),
                $ajaxSibling = $main.find('layer[data-id="'+$(this).data('id')+'"]');

            if($ajaxSibling.length) {
                var $tChildLayer = $(this).find('>layer'),
                    $sChildLayer = $ajaxSibling.find('>layer');
                if($sChildLayer.length && !$tChildLayer.length) {
                    $t[0].appendLayer($sChildLayer);
                } else if($tChildLayer.length && ($tChildLayer.data('id') != $sChildLayer.data('id'))) {
                    $tChildLayer[0].removeLayer(function() {
                        $t[0].appendLayer($sChildLayer);
                    });
                }
            } else {
                $(this).trigger('removeLayer');
            }
        });

        $layers = $($main[0]).find('layer');
    },
    initHtmlLayerTag: function() {
        var layerTag = document.createElement('layer');
        var layerContentTag = document.createElement('layer-content');
    },
    single: function() {
        this.vars = {
            $layerContent: $(this).find('>layer-content'),
            $layerContentHeightIndicator: $('<layer-height-indicator />')
        };

        if($(this).attr('disabled')) {
            return this;
        }
        this.vars.$layerContent.append( this.vars.$layerContentHeightIndicator);
        var $layerContentHeader = $(this).find('layer-content-header');
        this.vars.$handle = $layerContentHeader.length ? $layerContentHeader : this.vars.$layerContent;

        if(handle = $(this).attr('handle')) {
            this.vars.$handle = $(handle, this);
        }

        this.vars.$handle[0].minHeight = parseFloat(this.vars.$layerContent.css('min-height'));
        this.vars.$handle[0].maxHeight = this.vars.$layerContentHeightIndicator[0].offsetTop;

        this.vars.$handle[0].opened = ($(this).attr('opened')=='true') || false;

        this.vars.$handle[0].prevDirection = this.vars.$handle[0].opened ? -1 : 1;

        this.vars.$handle[0].$layer = $(this);
        this.vars.$handle[0].$layerContent = this.vars.$layerContent;

        this.vars.$layerContent.css({
            height: this.vars.$layerContent[0].opened?this.vars.$layerContent.height():this.vars.$layerContent[0].minHeight
        });


        this.vars.$handle[0].whileOpenClose =
            ($(this).attr('whileOpenClose')?$.map($(this).attr('whileOpenClose').split(','), function(id) {
                return $('#'+id);
            }):false);

        var _this = this;

        this.eventHandler = {
            touchstart: function(e, preventThisPropagation) {
                //e.preventDefault();
                e.stopImmediatePropagation();

                $(this).trigger('moveTransitionStatus', (($(this).height()/this.maxHeight)*100));

                this.maxHeight = this.$layer[0].vars.$layerContentHeightIndicator[0].offsetTop;

                if(preventThisPropagation) {
                    return;
                }

                $(this).addClass('touchstart');

                if(typeof e.detail != 'undefined') {
                    if(typeof e.detail.changedTouches != 'undefined') {
                        e.changedTouches = e.detail.changedTouches;
                    }
                }


                this.prevClientY = this.touchstartPositionY = (e.changedTouches ? (e.changedTouches[0].clientY-this.offsetTop) : 0);
                this.touchstartHeight = $(this).height();
                this.resistance = 1.1;

                        console.log('touchstartPositionY', this.touchstartPositionY);

                this.direction = this.opened?-1:1;

                if(!this.opened) {
                    $.each(this.whileOpenClose, function(i, $item) {
                        $item.trigger('closeLayer');
                    });
                }
                $(this).removeClass('open');
            },
            touchmove: function(e) {
                e.stopImmediatePropagation();
                $(this).removeClass('touchstart').addClass('touchmove');

                if(typeof e.detail != 'undefined') {
                    if(typeof e.detail.changedTouches != 'undefined') {
                        e.changedTouches = e.detail.changedTouches;
                    }
                }

                var currentClientY = e.changedTouches[0].clientY-this.offsetTop,
                    calc = currentClientY-this.touchstartPositionY,
                    calc = this.touchstartHeight+calc;

                this.direction = this.prevClientY > currentClientY ? -1 : 1;

                /*
                if(this.direction != this.prevDirection) {
                    console.log(this.direction, this.prevDirection);
                    return;
                }*/

                if(this.$layer.attr('invertTouch') == 'true') {
                    this.direction = this.direction*-1;
                    calc = (calc-this.touchstartHeight)*-1;
                }

                $(this).trigger('moveTransitionStatus', ((calc/this.maxHeight)*100));

                var targetPosition = this.direction === 1 ? this.maxHeight : this.minHeight;

                if(
                    (this.direction === 1 && calc >= this.maxHeight ) ||
                    (this.direction === -1 && calc <= this.minHeight )
                ) {
                    return;
                }

                this.$layerContent.css({
                    height: (calc)
                });

                this.resistance = this.resistance*.1;
                this.prevClientY = currentClientY;
            },
            touchend: function(e) {
                //e.preventDefault();
                /*
                if(this.direction != this.prevDirection) {
                    this.prevDirection = this.direction;
                    return;
                }*/

                this.prevDirection = this.direction;

                e.stopImmediatePropagation();
                this.$layerContent.removeClass('touchstart').removeClass('touchmove').addClass('touchend');
                this.touchstartPositionY = 0;

                if(this.direction === 1) {
                    this.$layerContent.parent().trigger('layer-opened');
                    var finalHeight = this.maxHeight;
                    this.opened = true;
                    this.$layerContent.trigger('moveTransitionStatus', 100);
                } else {
                    this.$layerContent.parent().trigger('layer-closed');
                    var finalHeight = this.minHeight;
                    this.opened = false;
                    this.$layerContent.trigger('moveTransitionStatus', 0);
                }

                this.$layerContent.css({
                    height: finalHeight
                }).one(transitionEndTrigger, function() {
                    $(this).removeClass('touchend');
                    if(_this.vars.$handle[0].opened) {
                        $(this).addClass('open');
                    }
                    $(window).trigger('layerChange');
                });
                $(window).trigger('layerChange');
            },
            touchSimulation: function(e) {
                var type = false;

                switch (e.type)
                {
                    case "mousedown":
                        this.isMouseDown = true;
                        type = "touchstart";
                        break;
                    case "mousemove":
                        if(this.isMouseDown) {
                            type = "touchmove";
                        } else {
                            type = false;
                        }
                        break;
                    case "mouseup":
                        this.isMouseDown = false;
                        type = "touchend";
                        if(this == window) {
                            window.currentTouchedElem.dispatchEvent(new Event('touchend'));
                            //$(window.currentTouchedElem).trigger('touchend');
                        }
                        break;
                    default: return;
                }



                if(type) {
                    window.currentTouchedElem = this;
                    var simulatedEvent = new CustomEvent(type, {
                        foo: 'dev',
                        detail: {
                            changedTouches: [{
                                clientX : e.clientX,
                                clientY : e.clientY
                            }]
                        }
                    });


                    simulatedEvent.test = 'test';

                    this.dispatchEvent(simulatedEvent);
                } else {
                    return;
                }
            }
        }

        if(this.vars.$layerContent[0].minHeight >= this.vars.$layerContent[0].maxHeight) {
            this.vars.$layerContent[0].dispatchEvent(new Event('touchend'));
            //this.vars.$layerContent.trigger('touchend', this.eventHandler.touchend);
            return this;
        }


        this.vars.$handle[0].addEventListener('touchstart', this.eventHandler.touchstart, { passive : true });
        this.vars.$handle[0].addEventListener('touchmove', this.eventHandler.touchmove, { passive : true });
        this.vars.$handle[0].addEventListener('touchend', this.eventHandler.touchend, { passive : true });
        this.vars.$handle[0].addEventListener('touchcancel', this.eventHandler.touchend, { passive : true });

        if(!NSINKA_Global.is_mobile()) {
            this.vars.$handle[0].isMouseDown = false;
            /*this.vars.$handle.on(
                'mousedown mousemove mouseup',
                this.eventHandler.touchSimulation
            );*/

            $.each(['mousedown', 'mousemove', 'mouseup'], function(i, eventType) {
                _this.vars.$handle[0].addEventListener(eventType, _this.eventHandler.touchSimulation, { passive : true });
            });

            window.dispatchEvent(new Event('mouseup'));
            //$(window).on('mouseup', this.eventHandler.touchSimulation);
        }

        $(this).on('closeLayer', function() {
            if(this.vars.$handle[0].opened) {
                this.vars.$handle[0].dispatchEvent(new Event('touchstart'));
                this.vars.$handle[0].dispatchEvent(new Event('touchend'));
                //this.vars.$layerContent.trigger('touchstart').trigger('touchend');
            }
        });

        $(this).on('openLayer', function() {
            if(!this.vars.$handle[0].opened) {
                this.vars.$handle[0].dispatchEvent(new Event('touchstart'));
                this.vars.$handle[0].dispatchEvent(new Event('touchend'));
            }
        });

        this.removeLayer = function(afterFn) {
            $(this).remove();
            if(typeof afterFn != 'undefined') {
                afterFn();
            }
        };

        this.appendLayer = function($layer) {
            $(this).append($layer);
            $layer.addClass('inactive');

            NSINKA_Layers.layerLinks($layer);

            $layer.each(NSINKA_Layers.single);
            setTimeout(function() {
                $layer.addClass('inactive--transition').removeClass('inactive').on(transitionEndTrigger, function() {
                    //$(this).removeClass('inactive--transition');
                });
            });
        };


    },

}

/**
* Description for NSINKA_Map
* @private
* @property NSINKA_Map
*/

var NSINKA_Map = {
    vars: {
        userGeoPosition: new Array(),
        directionsService: false,
        directionsDisplay: false,
        infoWindow: false,
        map: false,
        routes: [
            {
                title : 'Route XY',
                route: [ 1, 5, 8, 12]
            },
            {
                title : 'Eigene Route',
                type: 'custom'
            }
        ],
        currentRoute: 0,
        waypoints: [

            {
                title: 'Freie Spiel- und Sportvereinigung, (FSSV) am Adenauerring',
                street: 'Adenauerring 36',
                location: {
                    lat: 49.024210,
                    lng: 8.394464
                },
                inactive: true
            },
            {
                title: 'Ludwig Marum, Ludwig-Marum-Straße',
                street: 'Ludwig-Marum-Straße',
                location: {
                    lat: 49.013931,
                    lng: 8.368547
                },
                inactive: true
            },
            {
                title: 'Adolf-Hitler-Haus,Ritterstraße 28/30',
                street: 'Ritterstraße 28/30',
                location: {
                    lat: 49.004969,
                    lng: 8.397915
                },
                inactive: true
            },
            {
                title: 'Arbeiterbildungsverein',
                street: 'Wilhelmstraße 14',
                location: {
                    lat: 49.002942,
                    lng: 8.405408
                },
                inactive: true
            },
            {
                title: 'August-Dosenbach-Straße',
                street: 'August-Dosenbach-Straße',
                location: {
                    lat: 49.001133,
                    lng: 8.341120
                },
                inactive: true
            },
            {
                title: 'Badisches Innenministerium',
                street: 'Schlossplatz 19',
                location: {
                    lat: 49.010772,
                    lng: 8.401985
                },
                inactive: true
            },
            {
                title: 'Bankhaus Veit L. Homburger',
                street: 'Karlstraße 11',
                location: {
                    lat: 48.997198,
                    lng: 8.394041
                },
                inactive: true
            },
            {
                title: 'DWM Deutsche Waffen- und Munitionsfabrik AG',
                street: 'Lorenzstraße',
                location: {
                    lat: 49.001499,
                    lng: 8.383093
                },
                inactive: true
            },
            {
                title: 'Führer-Verlag',
                street: 'Kaiserstraße 133',
                location: {
                    lat: 49.009541,
                    lng: 8.405210
                },
                inactive: true
            },
            {
                title: 'Gefängnis Riefstahlstraße',
                street: 'Riefstahlstr.9',
                location: {
                    lat: 49.013871,
                    lng: 8.385056
                },
                inactive: true
            },
            {
                title: 'Gestapo-Hauptstelle',
                street: 'Ebertstr. 26',
                location: {
                    lat: 48.995090,
                    lng: 8.391867
                },
                inactive: true
            },
            {
                title: 'Hauptbahnhof',
                street: 'Bahnhofsplatz',
                location: {
                    lat: 49.014079,
                    lng: 8.429836
                },
                inactive: true
            },
            {
                title: 'Jüdischer Friedhof',
                street: 'Haid- und Neu-Str. 41, 45',
                location: {
                    lat: 49.014279,
                    lng: 8.431264
                },
                inactive: true
            },
            {
                title: 'Wohn- und ehemaliges Gemeindehaus',
                street: 'Herrenstraße 14',
                location: {
                    lat: 49.010436,
                    lng: 8.399910
                },
                inactive: true
            },
            {
                title: 'HJ und BDM',
                street: 'Rüppurrer Straße',
                location: {
                    lat: 49.000793,
                    lng: 8.410684
                },
                inactive: true
            },
                {
                title: 'Hotel Nassauer Hof ',
                street: 'Kriegsstr. 88',
                location: {
                    lat: 49.006021,
                    lng: 8.407203
                },
                inactive: true
            },
                {
                title: 'Jüdische Schule in der Lidellschule',
                street: 'Markgrafenstraße',
                location: {
                    lat: 49.007762,
                    lng: 8.408019
                },
                inactive: true
            },
                {
                title: 'Kaufhaus Hertie',
                street: 'Kaiserstraße 92',
                location: {
                    lat: 49.010098,
                    lng: 8.401213
                },
                inactive: true
            },
            {
                title: 'Kaufhaus Karstadt',
                street: 'Kaiserstraße 147-159',
                location: {
                    lat: 49.009523,
                    lng: 8.402030
                },
                inactive: true
            },
            {
                title: 'Ludwig-Marum-Straße',
                street: 'Ludwig-Marum-Straße',
                location: {
                    lat: 49.014304,
                    lng: 8.368282
                },
                inactive: true
            },
            {
                title: 'Marktplatz',
                street: 'Karl-Friedrich-Straße 7',
                location: {
                    lat: 49.009410,
                    lng: 8.403912
                },
                inactive: true
            },
            {
                title: 'Polizeipräsidium Marktplatz',
                street: '',
                location: {
                    lat: 49.010543,
                    lng: 8.402238
                },
                inactive: true
            },
            {
                title: 'Schlossplatz - Bücherverbrennung im Juni 1933',
                street: 'Schlossplatz',
                location: {
                    lat: 49.010999,
                    lng: 8.403854
                },
                inactive: true
            },
            {
                title: 'Staatliche Kunsthalle',
                street: 'Hans-Thoma-Str. 2-6',
                location: {
                    lat: 49.012006,
                    lng: 8.399949
                },
                inactive: true
            },
            {
                title: 'Synagoge in der - Karl-Friedrich-Straße 14-18',
                street: 'Karl-Friedrich-Straße 14-18',
                location: {
                    lat: 49.007876,
                    lng: 8.403517
                },
                inactive: true
            },
            {
                title: 'Synagoge Kronenstraße',
                street: 'Kronenstraße 15',
                location: {
                    lat: 49.009926,
                    lng: 8.408276
                },
                inactive: true
            },
            {
                title: 'Technische Hochschule',
                street: 'Kaiserstraße 12',
                location: {
                    lat: 49.009675,
                    lng: 8.411613
                },
                inactive: true
            },
            {
                title: '"Volkshaus"',
                street: 'Schützenstraße 14',
                location: {
                    lat: 49.002430,
                    lng: 8.404942
                },
                inactive: true
            },
            {
                title: 'Wohnhaus Kreuzstraße 6-8',
                street: 'Kreuzstraße 6-8',
                location: {
                    lat: 49.010146,
                    lng: 8.405229
                },
                inactive: true
            },
            {
                title: 'Zentralverband der Angestellten',
                street: 'Gartenstraße 25',
                location: {
                    lat: 49.004745,
                    lng: 8.392970
                },
                inactive: true
            },

        ],
        startPoint: '',
        endPoint: '',
        markers: {
            waypoints: [],
            waypoints_inactive: [],
            userLocation: [],
            windows: []
        },
        allMarkersBound: false,
        overlay: false
    },
    init: function() {
        var map = document.getElementById('map');

        if(!map) {
            return;
        }

        NSINKA_Map.vars.startPoint = NSINKA_Map.vars.waypoints[0].location;
        NSINKA_Map.vars.endPoint = NSINKA_Map.vars.waypoints[NSINKA_Map.vars.waypoints.length-1].location;

        NSINKA_Map.vars.directionsService = new google.maps.DirectionsService;

        NSINKA_Map.vars.allMarkersBound = new google.maps.LatLngBounds();

        NSINKA_Map.vars.map = new google.maps.Map(map, {
            center: {
                lat: 49.0157643,
                lng: 8.2698457
            },
            disableDefaultUI: true,
            zoomControl: true,
            zoomControlOptions: {
                position: google.maps.ControlPosition.LEFT_BOTTOM
            },
            zoom: 13
        });

        NSINKA_Map.vars.overlay = new google.maps.OverlayView();
        NSINKA_Map.vars.overlay.draw = function() {};
        NSINKA_Map.vars.overlay.setMap(NSINKA_Map.vars.map);


        $(window).on('layerChange', function() {
            google.maps.event.trigger(NSINKA_Map.vars.map, "resize");
            /*var listener = google.maps.event.addListener(NSINKA_Map.vars.map, "idle", function() {
                NSINKA_Map.vars.map.setZoom(13);
                google.maps.event.removeListener(listener);
            });  */
            NSINKA_Map.reCenter();
        });

        $.each($.extend(true, {}, NSINKA_Map.vars.waypoints), function(i, waypoint) {
            NSINKA_Map.vars.allMarkersBound.extend(new google.maps.LatLng(waypoint.location.lat, waypoint.location.lng));
        })

        //NSINKA_Map.vars.infoWindow = new google.maps.InfoWindow({map: NSINKA_Map.vars.map});
        NSINKA_Map.initRoute();
        NSINKA_Map.reCenter();
        NSINKA_Map.genUserLocationMaker();
        NSINKA_Map.genRoute();
    },
    initRoute: function(select) {
        NSINKA_Map.vars.currentRoute = select || NSINKA_Map.vars.currentRoute;

        $.each(NSINKA_Map.vars.waypoints, function(i, elem) {
            if($.inArray(i, NSINKA_Map.vars.routes[NSINKA_Map.vars.currentRoute].route) != -1) {
                NSINKA_Map.vars.waypoints[i].inactive = false;
            } else {
                NSINKA_Map.vars.waypoints[i].inactive = true;
            }
        });

    },
    initCustomRoute: function() {
        $.each(NSINKA_Map.vars.routes, function(i, route) {
            if(route.type) {
                if(route.type == 'custom') {
                    NSINKA_Map.vars.currentRoute = i;
                }
            }
        });
    },
    reCenter: function() {
        NSINKA_Map.getUserLocation(function(pos) {
            if(pos) {
                NSINKA_Map.vars.map.setCenter(new google.maps.LatLng(pos.coords.latitude,pos.coords.longitude));
            } else {
                NSINKA_Map.vars.map.fitBounds(NSINKA_Map.vars.allMarkersBound);
            }
        });
    },
    genUserLocationMaker: function() {

        var r = 6;
        NSINKA_Map.vars.userGeoPosition.background = new google.maps.Marker({
            clickable: false,
            icon: {
                path: 'M-'+(r)+',0a'+(r)+','+(r)+' 0 1,0 '+(r*2)+',0a'+(r)+','+(r)+' 0 1,0 -'+(r*2)+',0',
                fillColor: '#0066CC',
                fillOpacity: 1,
                anchor: new google.maps.Point(0,0),
                strokeColor: '#0066CC',
                strokeWeight: 40,
                strokeOpacity: .25,
                scale: 1
            },
            shadow: null,
            zIndex: 999,
            map: NSINKA_Map.vars.map
        });
        var s = 4;
        NSINKA_Map.vars.userGeoPosition.arrow = new google.maps.Marker({
            clickable: false,
            icon: {
                path: 'M 0 0 L '+(s*2)+' 0 L '+(s)+' '+(s*2)+' z',
                fillColor: '#0066CC',
                fillOpacity: 0,
                anchor: new google.maps.Point((s/2),((s/2)-(r+5))),
                strokeWeight: 0,
                scale: 1,
                rotation: 45
            },
            shadow: null,
            zIndex: 999,
            map: NSINKA_Map.vars.map
        });


        NSINKA_Map.vars.userGeoPosition.setPosition = function(pos, compass) {

            if(pos) {
                NSINKA_Map.vars.userGeoPosition.background.setPosition(pos);
                NSINKA_Map.vars.userGeoPosition.arrow.setPosition(pos);
            }

            if(compass) {
                var icon = NSINKA_Map.vars.userGeoPosition.arrow.getIcon()
                icon.rotation = compass-180;
                icon.fillOpacity = 1;
                NSINKA_Map.vars.userGeoPosition.arrow.setIcon(icon);
            }

        }

        // on compass change
        if (window.DeviceOrientationEvent) {
            window.addEventListener('deviceorientation', function(eventData) {
                var compassdir;
                if(event.webkitCompassHeading) {
                    compassdir = event.webkitCompassHeading;
                } else {
                    compassdir = event.alpha;
                }
                NSINKA_Map.vars.userGeoPosition.setPosition(null, compassdir);
                $(window).trigger('compassHeading', compassdir);
            });
        }

        navigator.geolocation.getCurrentPosition(this.setUserLocation, function() {
            console.error('no location');
        });

        // on position change
        navigator.geolocation.watchPosition(this.setUserLocation);


    },
    getUserLocation: function(fnAfter) {
        navigator.geolocation.getCurrentPosition(fnAfter, function() {
            fnAfter(false);
        });
    },

    setUserLocation: function(pos) {
        NSINKA_Map.vars.userGeoPosition.latLng = new google.maps.LatLng(pos.coords.latitude, pos.coords.longitude);
        NSINKA_Map.vars.userGeoPosition.setPosition(NSINKA_Map.vars.userGeoPosition.latLng);
    },
    genRoute: function() {
        var zoom = NSINKA_Map.vars.map.getZoom();
        if(NSINKA_Map.vars.directionsDisplay){
            //NSINKA_Map.vars.directionsService.setMap(null);
            NSINKA_Map.vars.directionsDisplay.setMap(null);
        }

        NSINKA_Map.vars.directionsDisplay = new google.maps.DirectionsRenderer({
            polylineOptions: {
                strokeColor: "#000000"
            },
            suppressMarkers: true
        });

        NSINKA_Map.vars.directionsDisplay.setMap(NSINKA_Map.vars.map);

        var waypoints_active = [],
            waypoints_inactive = [];

        $.each($.extend(true, {}, NSINKA_Map.vars.waypoints), function(i, waypoint) {
            if(typeof waypoint.inactive != 'undefined') {
                if(waypoint.inactive) {
                    waypoints_inactive.push(waypoint);
                    return;
                }
            }
            waypoints_active.push(waypoint);
        })

        var active_length = Object.keys(waypoints_active).length;


        if(active_length) {

                var start = NSINKA_Map.vars.userGeoPosition.latLng ?
                        NSINKA_Map.vars.userGeoPosition.latLng :
                        new google.maps.LatLng(waypoints_active[0].location),
                    end =
                        new google.maps.LatLng(waypoints_active[active_length-1].location)
                    ;


                this.vars.directionsService.route({
                    origin: start,
                    destination: end,
                    waypoints: $.map($.extend(true, {}, waypoints_active), function(waypoint) {
                        return {
                            location : waypoint.location
                        };
                    }),
                    optimizeWaypoints: true,
                    travelMode: google.maps.TravelMode.WALKING
                }, function(response, status) {
                    if (status === google.maps.DirectionsStatus.OK) {
                        NSINKA_Map.vars.directionsDisplay.setDirections(response);

                        response.described_waypoints = $.map(response.geocoded_waypoints, function(waypoint, i) {
                            return Object.assign(waypoint, waypoints_active[i]);
                        });

                        response.system_waypoints = NSINKA_Map.vars.waypoints;

                        $(window).trigger('routeHasBeenCalculated', response);

                        $.each(NSINKA_Map.vars.markers.waypoints, function(i, item) {
                            item.setMap(null);
                        });

                        var leg = response.routes[ 0 ].legs[ 0 ];
                        var r = 5;

                        /* Active markers */
                        var legs = response.routes[ 0 ].legs;
                        $.each(waypoints_active, function(i, marker) {
                            /*if(i == (legs.length-1) || !i) {
                                return;
                            }*/

                            var _marker = new google.maps.Marker({
                                position: new google.maps.LatLng(marker.location.lat, marker.location.lng),
                                map: NSINKA_Map.vars.map,
                                icon: {
                                    path: 'M-'+(r)+',0a'+(r)+','+(r)+' 0 1,0 '+(r*2)+',0a'+(r)+','+(r)+' 0 1,0 -'+(r*2)+',0',
                                    fillColor: '#f6ae95',
                                    fillOpacity: 1,
                                    anchor: new google.maps.Point(0,0),
                                    strokeWeight: 2,
                                    scale: 2,
                                }
                            });

                            _marker.nsinkaData = waypoints_active[i];

                            NSINKA_Map.vars.markers.waypoints.push(_marker);
                            NSINKA_Map.markerWindow(_marker);

                        });

                        setTimeout(function() {
                            NSINKA_Map.vars.map.setZoom(11);
                        });
                    } else {
                        window.alert('Directions request failed due to ' + status);
                    }
                });

        }
        /* Inactive markers */
        $.each($.extend(true, {}, waypoints_inactive), function(i, marker) {
            var r = 5;

            var _marker = new google.maps.Marker({
                position: new google.maps.LatLng(marker.location),
                icon: {
                    path: 'M-'+(r)+',0a'+(r)+','+(r)+' 0 1,0 '+(r*2)+',0a'+(r)+','+(r)+' 0 1,0 -'+(r*2)+',0',
                    fillColor: '#000000',
                    fillOpacity: .5,
                    anchor: new google.maps.Point(0,0),
                    strokeWeight: 0,
                    scale: 1,
                }
            });

            _marker.nsinkaData = marker;

            NSINKA_Map.markerWindow(_marker);

            NSINKA_Map.vars.markers.waypoints_inactive.push(_marker);

            _marker.setMap(NSINKA_Map.vars.map);

        });

        if(!Object.keys(waypoints_active).length) {
            setTimeout(function() {
                $(window).trigger('routeHasBeenCalculated', {});
            }, 1000)
        }
    },
    handleError: function(browserHasGeolocation, pos) {
        NSINKA_Map.vars.infoWindow.setPosition(pos);
        NSINKA_Map.vars.infoWindow.setContent(browserHasGeolocation ?
        'Error: The Geolocation service failed.' :
        'Error: Your browser doesn\'t support geolocation.');
    },
    markerWindow: function(marker) {
        marker.addListener('click', function() {
            var $win = $(NSINKA_Global.tmpl('mapWindow', marker.nsinkaData));

            NSINKA_Map.vars.map.panTo(marker.getPosition());
            google.maps.event.addListenerOnce(NSINKA_Map.vars.map, 'idle', function(){
                $('#placeholder').append($win);
                setPos($win);

                $win[0].event_drag = NSINKA_Map.vars.map.addListener('bounds_changed', function() {
                    $win.trigger('close')
                });
                $win[0].event_zoom = NSINKA_Map.vars.map.addListener('zoom_changed', function() {
                    $win.trigger('close')
                });

                $.each(NSINKA_Map.vars.markers.windows, function(i, window) {
                    NSINKA_Map.vars.markers.windows.trigger('close');
                });

                NSINKA_Map.vars.markers.windows = $win;

                $win.find('.close').click(function() {
                    $win.remove();
                });
                //infowindow.open(NSINKA_Map.vars.map, marker);


                $win.on('close', function() {
                    $(this).remove();
                    google.maps.event.removeListener(this.event_drag);
                    google.maps.event.removeListener(this.event_zoom);
                });

            });





        });


        function setPos($elem) {
            var gmap = NSINKA_Map.vars.map;
            var topRight=gmap.getProjection().fromLatLngToPoint(gmap.getBounds().getNorthEast ());
            var bottomLeft=gmap.getProjection().fromLatLngToPoint(gmap.getBounds().getSouthWest());
            var scale=Math.pow(2,gmap.getZoom());
            var worldPoint=gmap.getProjection().fromLatLngToPoint(marker.getPosition());
            var point = new google.maps.Point((worldPoint.x-bottomLeft.x)*scale,(worldPoint.y-topRight.y)*scale);

            $elem.css({
                left:( point.x - $elem.width()/2),
                top:(point.y - $elem.height() - 30)
            });

        }
    },
    startDirections: function() {



    }
}

function NSINKA_gMap() {
    NSINKA_Map.init();
}

/**
* Description for NSINKA_Layers
* @private
* @property NSINKA_Layers
*/
var NSINKA_Specifics = {
    vars: {
    },
    init: function() {
        NSINKA_Specifics.specifics.desktop.mapLayout = NSINKA_Specifics.specifics.tablet.mapLayout;

        $(window).on('smartphone-actions tablet-actions desktop-actions', this.actions.attach);
    },
    actions: {
        attach: function(e) {

            var actions = Object.assign({}, NSINKA_Specifics.specifics.smartphone),
                actions = Object.assign(actions, NSINKA_Specifics.specifics.tablet),
                actions = Object.assign(actions, NSINKA_Specifics.specifics.desktop);

            $.each(actions, function(i, action) {
                NSINKA_Specifics.actions.remove(action);
            });
            $.each(NSINKA_Specifics.specifics[e.type.replace(/-actions$/,'')], function(i, action) {
                $.each(action.action.split(','), function(i, _action) {
                    action.$elem.on(_action, action.fn);
                });
                action.$elem.trigger('_now');
            });
        },
        remove: function(action) {
            $.each(action.action.split(','), function(i, _action) {
                action.$elem.off(action.action, action.fn);
                if(typeof action.fnOnRemove == 'function') {
                    action.fnOnRemove();
                }
            });

        }
    },
    specifics: {
        smartphone: {

        },
        tablet: {
            mapLayout: {
                /*
                 * Set map layout
                 */
                $elem : $(window),
                action: 'resize,_now',
                fn : function() {
                    var $quicklinks = $('#quicklinks'),
                        $parent = $quicklinks.parent(),
                        $map = $('#map'),
                        $footer = $('#footer');


                    new ResizeSensor($quicklinks[0], function() {
                        $map.trigger('_resize');
                    });

                    $map.on('_resize', function() {
                        $map.css({
                            height: $(window).height()-$parent.offset().top-$quicklinks.height()
                        });
                        $footer.css({
                            height: $(window).height()-$parent.offset().top-$quicklinks.height(),
                        });
                    }).trigger('_resize');
                },
                fnOnRemove: function() {
                    var $quicklinks = $('#quicklinks'),
                        $map = $('#map'),
                        $footer = $('#footer');

                    new ResizeSensor.detach($quicklinks[0]);
                    $map.css({
                        height: ''
                    }).off('_resize');
                    $footer.css({
                        height: '',
                    }).off('_resize');
                }
            }
        },
        desktop: {
        },
    }
}

var NSINKA_Chronicle = {
    vars: {
    },
    init: function() {
        NSINKA_Chronicle.versions.tablet.isInited = false;
        $(window).on('resize__chronicle', function() {
            if(NSINKA_Global.is_smartphone()) {
                NSINKA_Chronicle.versions.mobile.init();
            } else {
                NSINKA_Chronicle.versions.tablet.init();
            }
        }).trigger('resize__chronicle');
    },
    versions: {
        mobile: {
            init: function() {
                this.layers.init();
            },
            layers: {
                init: function() {
                    var _this = this,
                        $handle = $('.chronicle--global>.chronicle__handle').first();

                    $handle[0].$layer = $handle.closest('article');
                    $handle[0].$layerContent = $handle[0].$layer.find('.article-content-wrapper');

                    $handle[0].minWidth = 0;
                    $handle[0].maxWidth = $handle[0].$layer[0].offsetWidth-$handle[0].offsetWidth;
                    $handle[0].opened = false;
                    $handle[0].touchstartWidth = $handle[0].maxWidth;

                    $handle[0].addEventListener('touchstart', this.eventHandler.touchstart, { passive : true });
                    $handle[0].addEventListener('touchmove', this.eventHandler.touchmove, { passive : true });
                    $handle[0].addEventListener('touchend', this.eventHandler.touchend, { passive : true });
                    $handle[0].addEventListener('touchcancel', this.eventHandler.touchend, { passive : true });

                    if(!NSINKA_Global.is_mobile()) {
                        $handle[0].isMouseDown = false;

                        $.each(['mousedown', 'mousemove', 'mouseup'], function(i, eventType) {
                            $handle[0].addEventListener(eventType, _this.eventHandler.touchSimulation, { passive : true });
                        });

                        window.dispatchEvent(new Event('mouseup'));
                    }
                },
                eventHandler: {
                    touchstart: function(e, preventThisPropagation) {
                        e.stopImmediatePropagation();

                        this.maxHeight = $(this).closest('article').width();

                        if(preventThisPropagation) {
                            return;
                        }

                        $(this).addClass('touchstart');

                        if(typeof e.detail != 'undefined') {
                            if(typeof e.detail.changedTouches != 'undefined') {
                                e.changedTouches = e.detail.changedTouches;
                            }
                        }

                        this.prevClientX = this.touchstartPositionX = (e.changedTouches ? (e.changedTouches[0].clientX-this.$layer.offset().left) : 0);
                        this.touchstartWidth = $(this).width();
                        this.resistance = 1.1;


                        console.log('touchstartPositionX', this.touchstartPositionX);

                        this.direction = this.opened?-1:1;

                        if(!this.opened) {
                            $.each(this.whileOpenClose, function(i, $item) {
                                $item.trigger('closeLayer');
                            });
                        }
                        $(this).removeClass('open');

                    },
                    touchmove: function(e) {
                        e.stopImmediatePropagation();
                        $(this).removeClass('touchstart').addClass('touchmove');

                        if(typeof e.detail != 'undefined') {
                            if(typeof e.detail.changedTouches != 'undefined') {
                                e.changedTouches = e.detail.changedTouches;
                            }
                        }

                        var currentClientX = e.changedTouches[0].clientX,
                            calc = currentClientX-(this.touchstartPositionX*2),
                            calc = this.touchstartWidth+calc;

                        //calc = e.changedTouches[0].clientX;
                        this.direction = this.prevClientX > currentClientX ? -1 : 1;

                        /*
                        if(this.direction != this.prevDirection) {
                            console.log(this.direction, this.prevDirection);
                            return;
                        }*/

                        if(this.$layer.attr('invertTouch') == 'true') {
                            this.direction = this.direction*-1;
                            calc = (calc-this.touchstartWidth)*-1;
                        }

                        var targetPosition = this.direction === 1 ? this.maxWidth : this.minWidth;

                        if(
                            (this.direction === 1 && calc >= this.maxWidth ) ||
                            (this.direction === -1 && calc <= this.minWidth )
                        ) {
                            return;
                        }

                        this.$layer.css({
                            'transform': 'translateX(' + calc + 'px)'
                        });

                        this.resistance = this.resistance*.1;
                        this.prevClientX = currentClientX;

                    },
                    touchend: function(e) {
                        //e.preventDefault();
                        /*
                        if(this.direction != this.prevDirection) {
                            this.prevDirection = this.direction;
                            return;
                        }*/

                        this.prevDirection = this.direction;

                        e.stopImmediatePropagation();
                        this.$layer.removeClass('touchstart').removeClass('touchmove').addClass('touchend');
                        this.touchstartPositionX = 0;

                        if(this.direction === 1) {
                            this.$layer.parent().trigger('layer-opened');
                            var finalWidth = this.maxWidth;
                            this.opened = true;
                            this.$layerContent.trigger('moveTransitionStatus', 100);
                        } else {
                            this.$layer.parent().trigger('layer-closed');
                            var finalWidth = this.minWidth;
                            this.opened = false;
                            this.$layerContent.trigger('moveTransitionStatus', 0);
                        }
                        var _this = this;
                        this.$layer.css({
                            'transform': 'translateX(' + finalWidth + 'px)'
                        }).one(transitionEndTrigger, function() {
                            $(this).removeClass('touchend');
                            if(_this.opened) {
                                $(this).addClass('open');
                            }
                            $(window).trigger('layerChange');
                        });
                        $(window).trigger('layerChange');

                    },
                    touchSimulation: function(e) {
                        var type = false;

                        switch (e.type)
                        {
                            case "mousedown":
                                this.isMouseDown = true;
                                type = "touchstart";
                                break;
                            case "mousemove":
                                if(this.isMouseDown) {
                                    type = "touchmove";
                                } else {
                                    type = false;
                                }
                                break;
                            case "mouseup":
                                this.isMouseDown = false;
                                type = "touchend";
                                if(this == window) {
                                    window.currentTouchedElem.dispatchEvent(new Event('touchend'));
                                    //$(window.currentTouchedElem).trigger('touchend');
                                }
                                break;
                            default: return;
                        }

                        if(type) {
                            window.currentTouchedElem = this;
                            var simulatedEvent = new CustomEvent(type, {
                                foo: 'dev',
                                detail: {
                                    changedTouches: [{
                                        clientX : e.clientX,
                                        clientY : e.clientY
                                    }]
                                }
                            });

                            simulatedEvent.test = 'test';

                            this.dispatchEvent(simulatedEvent);
                        } else {
                            return;
                        }
                    }

                }
            }
        },
        tablet: {
            init: function() {
                if(!this.isInited) {
                    this.isInited = true;
                    this.prepareData();
                    this.scrollSync();
                    this.range();
                }
            },
            prepareData: function() {
                $('.chronicle__list').each(function() {
                    var $chronicleList = $(this),
                        data = {};

                    $('dt', this).each(function() {
                        var $this = $(this),
                            $dd = $this.next('dd'),
                            thisYear = $(this).text()*1;

                        data[thisYear] = {};

                        $dd.find('.chronicle__list__month').each(function() {
                            var $this = $(this),
                                thisMonth = $this.data('month'),
                                thisTeaser = $this.data('teaser') || ($this.text().substr(0, 200)+' …');

                            if(typeof data[thisYear][thisMonth] == 'undefined') {
                                data[thisYear][thisMonth] = [];
                            }

                            data[thisYear][thisMonth].push({
                                teaser : thisTeaser,
                                content : $this.html()
                            });
                        });

                        for(var i = 1; i <= 12; i++) {
                            var $monthDiv = $('<div class="chronicle__list__month chronicle__list__month--tiny">');
                            $dd.append($monthDiv);
                            $.each(data[thisYear][i], function(i, entries) {
                                var $dot = $('<div class="chronicle__list__month__dot">');
                                $monthDiv.append($dot);
                            });
                        }
                    });

                    console.log(data);
                });
            },
            scrollSync: function() {
                $('.chronicle__container').scroll(function() {
                    var $this = $(this),
                        $article = $this.closest('.content'),
                        $containers = $article.find('.chronicle__container').not($this);

                    $containers.scrollLeft($this.scrollLeft());
                });
            },
            range: function() {

                $('input[type="range"]').rangeslider({
                    polyfill: false,

                    //handleClass: 'chronicle__rangeslider__handle'
                });
            }
        }
    }
}

//# sourceMappingURL=data:application/json;charset=utf8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIl9fYXBwLmpzIiwiYmFzZS5qcyIsImNvbXBvbmVudHMvYWN0aW9ucy5qcyIsImNvbXBvbmVudHMvalF1ZXJ5RXh0ZW5zaW9ucy5qcyIsImNvbXBvbmVudHMvbGF5ZXJzLmpzIiwiY29tcG9uZW50cy9tYXAuanMiLCJjb21wb25lbnRzL3NwZWNpZmljcy5qcyIsImxheWVyLWNhc2VzL2FydGljbGUtY2hyb25pY2xlLmpzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQ1JBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQ2xHQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FDek5BO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FDeEJBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQzdXQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUNqckJBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FDMUZBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EiLCJmaWxlIjoiYXBwLmpzIiwic291cmNlc0NvbnRlbnQiOlsidmFyIHRyYW5zaXRpb25FbmRUcmlnZ2VyID0gJ3RyYW5zaXRpb25lbmQgd2Via2l0VHJhbnNpdGlvbkVuZCBvVHJhbnNpdGlvbkVuZCBNU1RyYW5zaXRpb25FbmQnO1xuXG4kKHdpbmRvdykucmVhZHkoZnVuY3Rpb24oKSB7XG4gICAgRl9HbG9iYWwuaW5pdCgpO1xufSkuc2Nyb2xsKGZ1bmN0aW9uKCkge1xufSkucmVzaXplKGZ1bmN0aW9uKCkge1xuXG59KTtcbiIsIi8qKlxuKiBEZXNjcmlwdGlvbiBmb3IgR09fR2xvYmFsXG4qIEBwcml2YXRlXG4qIEBwcm9wZXJ0eSBHT19HbG9iYWxcbiovXG52YXIgRl9HbG9iYWwgPSB7XG4gICAgdmFyczoge1xuICAgIH0sXG4gICAgLyoqXG4gICAgKiBEZXNjcmlwdGlvbiBmb3IgaW5pdFxuICAgICogQHByaXZhdGVcbiAgICAqIEBtZXRob2QgaW5pdFxuICAgICogQHJldHVybiB7T2JqZWN0fSBkZXNjcmlwdGlvblxuICAgICovXG4gICAgaW5pdDogZnVuY3Rpb24oKSB7XG5cbiAgICB9LFxuICAgIC8qKlxuICAgICogRGVzY3JpcHRpb24gZm9yIGlzX21vYmlsZVxuICAgICogQHByaXZhdGVcbiAgICAqIEBtZXRob2QgaXNfbW9iaWxlXG4gICAgKiBAcmV0dXJuIHtPYmplY3R9IGRlc2NyaXB0aW9uXG4gICAgKi9cbiAgICBpc19tb2JpbGU6IGZ1bmN0aW9uKCkge1xuICAgICAgICBpZihOU0lOS0FfR2xvYmFsLnZhcnMuaXNfbW9iaWxlICE9IG51bGwpIHtcbiAgICAgICAgICAgIHJldHVybiBOU0lOS0FfR2xvYmFsLnZhcnMuaXNfbW9iaWxlO1xuICAgICAgICB9XG5cbiAgICAgICAgdmFyIGNoZWNrID0gZmFsc2U7XG4gICAgICAgIChmdW5jdGlvbihhKXtpZigvKGFuZHJvaWR8YmJcXGQrfG1lZWdvKS4rbW9iaWxlfGF2YW50Z298YmFkYVxcL3xibGFja2JlcnJ5fGJsYXplcnxjb21wYWx8ZWxhaW5lfGZlbm5lY3xoaXB0b3B8aWVtb2JpbGV8aXAoaG9uZXxvZHxhZCl8aXJpc3xraW5kbGV8bGdlIHxtYWVtb3xtaWRwfG1tcHxtb2JpbGUuK2ZpcmVmb3h8bmV0ZnJvbnR8b3BlcmEgbShvYnxpbilpfHBhbG0oIG9zKT98cGhvbmV8cChpeGl8cmUpXFwvfHBsdWNrZXJ8cG9ja2V0fHBzcHxzZXJpZXMoNHw2KTB8c3ltYmlhbnx0cmVvfHVwXFwuKGJyb3dzZXJ8bGluayl8dm9kYWZvbmV8d2FwfHdpbmRvd3MgY2V8eGRhfHhpaW5vL2kudGVzdChhKXx8LzEyMDd8NjMxMHw2NTkwfDNnc298NHRocHw1MFsxLTZdaXw3NzBzfDgwMnN8YSB3YXxhYmFjfGFjKGVyfG9vfHNcXC0pfGFpKGtvfHJuKXxhbChhdnxjYXxjbyl8YW1vaXxhbihleHxueXx5dyl8YXB0dXxhcihjaHxnbyl8YXModGV8dXMpfGF0dHd8YXUoZGl8XFwtbXxyIHxzICl8YXZhbnxiZShja3xsbHxucSl8YmkobGJ8cmQpfGJsKGFjfGF6KXxicihlfHYpd3xidW1ifGJ3XFwtKG58dSl8YzU1XFwvfGNhcGl8Y2N3YXxjZG1cXC18Y2VsbHxjaHRtfGNsZGN8Y21kXFwtfGNvKG1wfG5kKXxjcmF3fGRhKGl0fGxsfG5nKXxkYnRlfGRjXFwtc3xkZXZpfGRpY2F8ZG1vYnxkbyhjfHApb3xkcygxMnxcXC1kKXxlbCg0OXxhaSl8ZW0obDJ8dWwpfGVyKGljfGswKXxlc2w4fGV6KFs0LTddMHxvc3x3YXx6ZSl8ZmV0Y3xmbHkoXFwtfF8pfGcxIHV8ZzU2MHxnZW5lfGdmXFwtNXxnXFwtbW98Z28oXFwud3xvZCl8Z3IoYWR8dW4pfGhhaWV8aGNpdHxoZFxcLShtfHB8dCl8aGVpXFwtfGhpKHB0fHRhKXxocCggaXxpcCl8aHNcXC1jfGh0KGMoXFwtfCB8X3xhfGd8cHxzfHQpfHRwKXxodShhd3x0Yyl8aVxcLSgyMHxnb3xtYSl8aTIzMHxpYWMoIHxcXC18XFwvKXxpYnJvfGlkZWF8aWcwMXxpa29tfGltMWt8aW5ub3xpcGFxfGlyaXN8amEodHx2KWF8amJyb3xqZW11fGppZ3N8a2RkaXxrZWppfGtndCggfFxcLyl8a2xvbnxrcHQgfGt3Y1xcLXxreW8oY3xrKXxsZShub3x4aSl8bGcoIGd8XFwvKGt8bHx1KXw1MHw1NHxcXC1bYS13XSl8bGlid3xseW54fG0xXFwtd3xtM2dhfG01MFxcL3xtYSh0ZXx1aXx4byl8bWMoMDF8MjF8Y2EpfG1cXC1jcnxtZShyY3xyaSl8bWkobzh8b2F8dHMpfG1tZWZ8bW8oMDF8MDJ8Yml8ZGV8ZG98dChcXC18IHxvfHYpfHp6KXxtdCg1MHxwMXx2ICl8bXdicHxteXdhfG4xMFswLTJdfG4yMFsyLTNdfG4zMCgwfDIpfG41MCgwfDJ8NSl8bjcoMCgwfDEpfDEwKXxuZSgoY3xtKVxcLXxvbnx0Znx3Znx3Z3x3dCl8bm9rKDZ8aSl8bnpwaHxvMmltfG9wKHRpfHd2KXxvcmFufG93ZzF8cDgwMHxwYW4oYXxkfHQpfHBkeGd8cGcoMTN8XFwtKFsxLThdfGMpKXxwaGlsfHBpcmV8cGwoYXl8dWMpfHBuXFwtMnxwbyhja3xydHxzZSl8cHJveHxwc2lvfHB0XFwtZ3xxYVxcLWF8cWMoMDd8MTJ8MjF8MzJ8NjB8XFwtWzItN118aVxcLSl8cXRla3xyMzgwfHI2MDB8cmFrc3xyaW05fHJvKHZlfHpvKXxzNTVcXC98c2EoZ2V8bWF8bW18bXN8bnl8dmEpfHNjKDAxfGhcXC18b298cFxcLSl8c2RrXFwvfHNlKGMoXFwtfDB8MSl8NDd8bWN8bmR8cmkpfHNnaFxcLXxzaGFyfHNpZShcXC18bSl8c2tcXC0wfHNsKDQ1fGlkKXxzbShhbHxhcnxiM3xpdHx0NSl8c28oZnR8bnkpfHNwKDAxfGhcXC18dlxcLXx2ICl8c3koMDF8bWIpfHQyKDE4fDUwKXx0NigwMHwxMHwxOCl8dGEoZ3R8bGspfHRjbFxcLXx0ZGdcXC18dGVsKGl8bSl8dGltXFwtfHRcXC1tb3x0byhwbHxzaCl8dHMoNzB8bVxcLXxtM3xtNSl8dHhcXC05fHVwKFxcLmJ8ZzF8c2kpfHV0c3R8djQwMHx2NzUwfHZlcml8dmkocmd8dGUpfHZrKDQwfDVbMC0zXXxcXC12KXx2bTQwfHZvZGF8dnVsY3x2eCg1Mnw1M3w2MHw2MXw3MHw4MHw4MXw4M3w4NXw5OCl8dzNjKFxcLXwgKXx3ZWJjfHdoaXR8d2koZyB8bmN8bncpfHdtbGJ8d29udXx4NzAwfHlhc1xcLXx5b3VyfHpldG98enRlXFwtL2kudGVzdChhLnN1YnN0cigwLDQpKSkgY2hlY2sgPSB0cnVlO30pKG5hdmlnYXRvci51c2VyQWdlbnR8fG5hdmlnYXRvci52ZW5kb3J8fHdpbmRvdy5vcGVyYSk7XG4gICAgICAgIE5TSU5LQV9HbG9iYWwudmFycy5pc19tb2JpbGUgPSBjaGVjaztcbiAgICAgICAgcmV0dXJuIGNoZWNrO1xuICAgIH0sXG4gICAgaXNfdGFibGV0OiBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHdXID0gJCh3aW5kb3cpLndpZHRoKCksXG4gICAgICAgICAgICB3SCA9ICQod2luZG93KS5oZWlnaHQoKSxcbiAgICAgICAgICAgIGZhY3RvciA9IHdXPndIP3dXL3dIOndIL3dXO1xuICAgICAgICByZXR1cm4gKFxuICAgICAgICAgICAgdGhpcy5pc19tb2JpbGUoKSAmJlxuICAgICAgICAgICAgLy8kKHdpbmRvdykud2lkdGgoKSA+IDQxNCAmJlxuICAgICAgICAgICAgZmFjdG9yIDwgMS42XG4gICAgICAgICk7XG4gICAgfSxcbiAgICBpc19zbWFydHBob25lOiBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHdXID0gJCh3aW5kb3cpLndpZHRoKCksXG4gICAgICAgICAgICB3SCA9ICQod2luZG93KS5oZWlnaHQoKSxcbiAgICAgICAgICAgIGZhY3RvciA9IHdXPndIP3dXL3dIOndIL3dXO1xuICAgICAgICByZXR1cm4gKFxuICAgICAgICAgICAgdGhpcy5pc19tb2JpbGUoKSAmJlxuICAgICAgICAgICAgLy8kKHdpbmRvdykud2lkdGgoKSA+IDQxNCAmJlxuICAgICAgICAgICAgZmFjdG9yID4gMS42XG4gICAgICAgICk7XG4gICAgfSxcbiAgICBhamF4UGFnZTogZnVuY3Rpb24odXJsLCBmbkFmdGVyKSB7XG4gICAgICAgIGlmKE5TSU5LQV9HbG9iYWwudmFycy54aHIpIHtcbiAgICAgICAgICAgIE5TSU5LQV9HbG9iYWwudmFycy54aHIuYWJvcnQoKTtcbiAgICAgICAgfVxuXG4gICAgICAgIE5TSU5LQV9HbG9iYWwudmFycy54aHIgPSAkLmFqYXgoe1xuICAgICAgICAgICAgdXJsIDogdXJsLFxuICAgICAgICAgICAgc3VjY2VzczogZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgICAgICAgICRkYXRhID0gJC5wYXJzZUhUTUwoZGF0YSkuZmlsdGVyKGZ1bmN0aW9uKGl0ZW0pIHtcbiAgICAgICAgICAgICAgICAgICAgaWYoaXRlbS5ub2RlTmFtZSA9PSAnVElUTEUnIHx8IGl0ZW0ubm9kZU5hbWUgPT0gJ01BSU4nKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gaXRlbTtcbiAgICAgICAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgICAgICAgdGl0bGUgPSAkZGF0YS5maWx0ZXIoZnVuY3Rpb24oaXRlbSkge1xuICAgICAgICAgICAgICAgICAgICBpZihpdGVtLm5vZGVOYW1lID09ICdUSVRMRScpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiBpdGVtO1xuICAgICAgICAgICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSk7XG5cbiAgICAgICAgICAgICAgICAkKCd0aXRsZScpLnRleHQoJCh0aXRsZVswXSkudGV4dCgpKTtcblxuICAgICAgICAgICAgICAgIGZuQWZ0ZXIoJGRhdGEpO1xuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIGRhdGFUeXBlOiBcImh0bWxcIixcbiAgICAgICAgfSk7XG4gICAgfSxcbiAgICAvKipcbiAgICAqIERlc2NyaXB0aW9uIGZvciB0bXBsXG4gICAgKiBAcHJpdmF0ZVxuICAgICogQG1ldGhvZCB0bXBsXG4gICAgKiBAcGFyYW0ge09iamVjdH0gaWQgKHJlcXVpcmVkKVxuICAgICogQHBhcmFtIHtPYmplY3R9IGRhdGEgKGRlZmF1bHQ6IG51bGwpXG4gICAgKiBAcmV0dXJuIHtPYmplY3R9IGpRdWVyeSBlbGVtZW50c1xuICAgICovXG4gICAgdG1wbDogZnVuY3Rpb24oaWQsIGRhdGEpIHtcbiAgICAgICAgZGF0YSA9IHsgZGF0YSA6IGRhdGEgfSB8fCB7IGRhdGEgOiB7fSB9O1xuXG4gICAgICAgIHJldHVybiBfLnRlbXBsYXRlKCQoJyN0bXBsXycraWQpLmNsb25lKCkudGV4dCgpKShkYXRhKTtcbiAgICB9XG59XG4iLCIvKipcbiogRGVzY3JpcHRpb24gZm9yIE5TSU5LQV9BY3Rpb25zXG4qIEBwcml2YXRlXG4qIEBwcm9wZXJ0eSBOU0lOS0FfQWN0aW9uc1xuKi9cbnZhciBOU0lOS0FfQWN0aW9ucyA9IHtcbiAgICB2YXJzOiB7XG4gICAgfSxcbiAgICBpbml0OiBmdW5jdGlvbigkYm9keSkge1xuICAgICAgICAkYm9keSA9ICRib2R5IHx8ICQoJ2JvZHknKTtcbiAgICAgICAgJCgnKltkYXRhLWpzYWN0aW9uXScsICRib2R5KS5hZGQoJGJvZHkuZmlsdGVyKCdbZGF0YS1qc2FjdGlvbl0nKSkuZWFjaCh0aGlzLl9fdHJpZ2dlckFjdGlvbik7XG4gICAgfSxcbiAgICBfX3RyaWdnZXJBY3Rpb246IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgJHRoaXMgPSAkKHRoaXMpLFxuICAgICAgICAgICAganNhY3Rpb24gPSAkdGhpcy5kYXRhKCdqc2FjdGlvbicpO1xuXG4gICAgICAgICQuZWFjaChqc2FjdGlvbi5tYXRjaCgvXFx3KyhcXChbXlxcKV0rXFwpKT8vZyksIGZ1bmN0aW9uKGksIGZuKSB7XG4gICAgICAgICAgICB2YXIgYXR0cnMgPSBmbi5yZXBsYWNlKC9eXFx3K1xcKHxcXCkkL2csICcnKS5zcGxpdCgnLCcpLFxuICAgICAgICAgICAgICAgIGZuID0gZm4ubWF0Y2goL15cXHcrLylbMF07XG4gICAgICAgICAgICBpZih0eXBlb2YgTlNJTktBX0FjdGlvbnMuZnVuY3Rpb25zW2ZuXSAhPSAndW5kZWZpbmVkJykge1xuICAgICAgICAgICAgICAgIE5TSU5LQV9BY3Rpb25zLmZ1bmN0aW9uc1tmbl0oJHRoaXMsIGF0dHJzKTtcbiAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgY29uc29sZS5lcnJvcignbm90IGZvdW5kISBmdW5jdGlvbiBOU0lOS0FfQWN0aW9ucy5mdW5jdGlvbnMuJytmbisnKCcrYXR0cnMuam9pbignLCcpKycpJyk7XG4gICAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgIH0sXG4gICAgZnVuY3Rpb25zOiB7XG4gICAgICAgIG9wZW5tZW51OiBmdW5jdGlvbigkdGhpcywgYXR0cnMpIHtcbiAgICAgICAgICAgICR0aGlzLmNsaWNrdG91Y2goZnVuY3Rpb24oZSkge1xuICAgICAgICAgICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICAgICAgICBlLnN0b3BJbW1lZGlhdGVQcm9wYWdhdGlvbigpO1xuICAgICAgICAgICAgICAgIHZhciAkaHRtbCA9ICQoJ2h0bWwnKSxcbiAgICAgICAgICAgICAgICAgICAgb3BlbmVkX2NsYXNzID0gJ25hdl9vcGVuJyxcbiAgICAgICAgICAgICAgICAgICAgYnV0dG9uX2FjdGl2ZV9jbGFzcyA9ICdpcy1hY3RpdmUnO1xuICAgICAgICAgICAgICAgIGlmKCRodG1sLmhhc0NsYXNzKG9wZW5lZF9jbGFzcykpIHtcbiAgICAgICAgICAgICAgICAgICAgJHRoaXMucmVtb3ZlQ2xhc3MoYnV0dG9uX2FjdGl2ZV9jbGFzcyk7XG4gICAgICAgICAgICAgICAgICAgICRodG1sLnJlbW92ZUNsYXNzKG9wZW5lZF9jbGFzcyk7XG4gICAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgJHRoaXMuYWRkQ2xhc3MoYnV0dG9uX2FjdGl2ZV9jbGFzcyk7XG4gICAgICAgICAgICAgICAgICAgICRodG1sLmFkZENsYXNzKG9wZW5lZF9jbGFzcyk7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfSk7XG4gICAgICAgIH0sXG4gICAgICAgIHRyYW5zZm9ybWxvZ286IGZ1bmN0aW9uKCR0aGlzKSB7XG4gICAgICAgICAgICAkdGhpcy5vbignbW92ZVRyYW5zaXRpb25TdGF0dXMnLCBmdW5jdGlvbihlLCBwZXJjZW50YWdlKSB7XG4gICAgICAgICAgICAgICAgaWYocGVyY2VudGFnZT4wKSB7XG4gICAgICAgICAgICAgICAgICAgICR0aGlzLmNsb3Nlc3QoJ2xheWVyJykucmVtb3ZlQ2xhc3MoJ3RyYW5zZm9ybWVkJykuZmluZCgnI2xvZ29fX2NvbnRhaW5lciBzcGFuJykvKi5mYWRlSW4oKSovO1xuICAgICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgICR0aGlzLmNsb3Nlc3QoJ2xheWVyJykuYWRkQ2xhc3MoJ3RyYW5zZm9ybWVkJykuZmluZCgnI2xvZ29fX2NvbnRhaW5lciBzcGFuJykvKi5mYWRlT3V0KGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgLy8kKHRoaXMpLmhpZGUoKTtcbiAgICAgICAgICAgICAgICAgICAgfSkqLztcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICAgICAgY2xvbmVIZWFkZXI6IGZ1bmN0aW9uKCR0aGlzKSB7XG4gICAgICAgICAgICAkKHdpbmRvdykub24oJ3Njcm9sbF9faGVhZGVyJywgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgaWYoJCh3aW5kb3cpLnNjcm9sbFRvcCgpPjIpIHtcbiAgICAgICAgICAgICAgICAgICAgJHRoaXMudHJpZ2dlcignY2xvc2VMYXllcicpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgICAvL3ZhciAkbG9nbyA9ICR0aGlzLmZpbmQoJyNsb2dvX19jb250YWluZXInKTtcbiAgICAgICAgICAgIC8vJGxvZ28uYWZ0ZXIoJGxvZ28uY2xvbmUodHJ1ZSx0cnVlKS5hZGRDbGFzcygnY2xvbmUnKSk7XG4gICAgICAgIH0sXG4gICAgICAgIHN0YXJ0RGlyZWN0aW9uczogZnVuY3Rpb24oJHRoaXMpIHtcbiAgICAgICAgICAgICR0aGlzLmNsaWNrdG91Y2goTlNJTktBX01hcC5zdGFydERpcmVjdGlvbnMpO1xuICAgICAgICB9LFxuICAgICAgICByZWNlaXZlUm91dGVEdXJhdGlvbjogZnVuY3Rpb24oJHRoaXMpIHtcbiAgICAgICAgICAgICQod2luZG93KS5vbigncm91dGVIYXNCZWVuQ2FsY3VsYXRlZCcsIGZ1bmN0aW9uKGUsIHJvdXRlKSB7XG4gICAgICAgICAgICAgICAgY2FsY0R1cmF0aW9uKHJvdXRlKTtcbiAgICAgICAgICAgICAgICBzaG93V2F5cG9pbnRzKHJvdXRlKTtcbiAgICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgICBmdW5jdGlvbiBjYWxjRHVyYXRpb24ocm91dGUpIHtcbiAgICAgICAgICAgICAgICB2YXIgZHVyYXRpb24gPSAwO1xuXG4gICAgICAgICAgICAgICAgaWYoIU9iamVjdC5rZXlzKHJvdXRlKS5sZW5ndGgpIHtcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgICAgICAgIH1cblxuICAgICAgICAgICAgICAgICQuZWFjaChyb3V0ZS5yb3V0ZXNbMF0ubGVncywgZnVuY3Rpb24oaSwgd2F5cG9pbnQpIHtcbiAgICAgICAgICAgICAgICAgICAgZHVyYXRpb24gKz0gd2F5cG9pbnQuZHVyYXRpb24udmFsdWU7XG4gICAgICAgICAgICAgICAgfSk7XG5cbiAgICAgICAgICAgICAgICAkdGhpcy50ZXh0KChNYXRoLnJvdW5kKChkdXJhdGlvbi82MC82MCkqNCkvNCkrJyBTdGQuJyk7XG5cbiAgICAgICAgICAgIH1cblxuICAgICAgICAgICAgZnVuY3Rpb24gc2hvd1dheXBvaW50cyhyb3V0ZSkge1xuICAgICAgICAgICAgICAgIHZhciAkdG1wbCA9ICQoTlNJTktBX0dsb2JhbC50bXBsKCdyb3V0ZVNldHRpbmdzJywge1xuICAgICAgICAgICAgICAgICAgICBkaXJlY3Rpb25zOiBOU0lOS0FfTWFwLnZhcnMud2F5cG9pbnRzLFxuICAgICAgICAgICAgICAgICAgICByb3V0ZXM6IE5TSU5LQV9NYXAudmFycy5yb3V0ZXMsXG4gICAgICAgICAgICAgICAgICAgIGN1cnJlbnRSb3V0ZTogTlNJTktBX01hcC52YXJzLmN1cnJlbnRSb3V0ZSxcbiAgICAgICAgICAgICAgICB9KSk7XG5cbiAgICAgICAgICAgICAgICAkdGhpcy5jbG9zZXN0KCdsYXllci1jb250ZW50JykuZmluZCgnbGF5ZXItY29udGVudC1vcGVuZWQnKS5odG1sKCcnKS5hcHBlbmQoJHRtcGwpO1xuXG4gICAgICAgICAgICAgICAgJHRtcGwuZmluZCgnbGknKS5lYWNoKGZ1bmN0aW9uKGkpIHtcbiAgICAgICAgICAgICAgICAgICAgdGhpcy5tYXBEYXRhID0gTlNJTktBX01hcC52YXJzLndheXBvaW50c1tpXTtcbiAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgICAgICR0bXBsLmZpbmQoJ3VsJykuc29ydGFibGUoe1xuICAgICAgICAgICAgICAgICAgICBwbGFjZWhvbGRlcjogXCJwbGFjZWhvbGRlclwiLFxuICAgICAgICAgICAgICAgICAgICBzdG9wOiBmdW5jdGlvbihldmVudCx1aSl7XG4gICAgICAgICAgICAgICAgICAgICAgICBOU0lOS0FfTWFwLnZhcnMud2F5cG9pbnRzID0gW107XG4gICAgICAgICAgICAgICAgICAgICAgICAkdG1wbC5maW5kKCdsaScpLmVhY2goZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLndheXBvaW50cy5wdXNoKHRoaXMubWFwRGF0YSk7XG4gICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgICAgIE5TSU5LQV9NYXAuaW5pdEN1c3RvbVJvdXRlKCk7XG4gICAgICAgICAgICAgICAgICAgICAgICBOU0lOS0FfTWFwLmdlblJvdXRlKCk7XG4gICAgICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgICAgIGF4aXM6IFwieVwiLFxuICAgICAgICAgICAgICAgICAgICBoZWxwZXI6ICdjbG9uZScsXG4gICAgICAgICAgICAgICAgICAgIGhhbmRsZTogJy5kcmFnLWhhbmRsZSdcbiAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgICAgIE5TSU5LQV9BY3Rpb25zLmluaXQoJHRtcGwpO1xuICAgICAgICAgICAgfVxuICAgICAgICB9LFxuICAgICAgICByb3V0ZVNldHRpbmdzOiBmdW5jdGlvbigkdGhpcykge1xuICAgICAgICAgICAgJHRoaXMuZmluZCgnYS5kZWFjdGl2YXRlJykuY2xpY2soZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgdmFyIGRlYWN0aXZhdGVkQ2xhc3MgPSAnZGVhY3RpdmF0ZWQnLFxuICAgICAgICAgICAgICAgICAgICAkdGhpcyA9ICQodGhpcyksXG4gICAgICAgICAgICAgICAgICAgICRyb3cgPSAkdGhpcy5jbG9zZXN0KCdsaScpO1xuXG5cbiAgICAgICAgICAgICAgICBpZigkcm93Lmhhc0NsYXNzKGRlYWN0aXZhdGVkQ2xhc3MpKSB7XG4gICAgICAgICAgICAgICAgICAgIHZhciBpbmFjdGl2ZSA9IGZhbHNlO1xuICAgICAgICAgICAgICAgICAgICAkcm93LnJlbW92ZUNsYXNzKGRlYWN0aXZhdGVkQ2xhc3MpO1xuICAgICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgIHZhciBpbmFjdGl2ZSA9IHRydWU7XG4gICAgICAgICAgICAgICAgICAgICRyb3cuYWRkQ2xhc3MoZGVhY3RpdmF0ZWRDbGFzcyk7XG4gICAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgICAgJGNoYW5nZUljb24gPSAkdGhpcy5kYXRhKCdjaGFuZ2UtaWNvbicpO1xuICAgICAgICAgICAgICAgICR0aGlzLmRhdGEoJ2NoYW5nZS1pY29uJywgJHRoaXMudGV4dCgpKTtcbiAgICAgICAgICAgICAgICAkdGhpcy50ZXh0KCRjaGFuZ2VJY29uKTtcblxuICAgICAgICAgICAgICAgIE5TSU5LQV9NYXAudmFycy53YXlwb2ludHNbJHJvdy5kYXRhKCdsb2NhdGlvbi1pZCcpXS5pbmFjdGl2ZSA9IGluYWN0aXZlO1xuXG4gICAgICAgICAgICAgICAgTlNJTktBX01hcC5pbml0Q3VzdG9tUm91dGUoKTtcbiAgICAgICAgICAgICAgICBOU0lOS0FfTWFwLmdlblJvdXRlKCk7XG5cbiAgICAgICAgICAgIH0pXG4gICAgICAgIH0sXG4gICAgICAgIHNlYXJjaDogZnVuY3Rpb24oJHRoaXMpIHtcbiAgICAgICAgICAgICR0aGlzLmZpbmQoJ2J1dHRvbicpLmNsaWNrdG91Y2goZnVuY3Rpb24oZSkge1xuICAgICAgICAgICAgICAgIGUuc3RvcEltbWVkaWF0ZVByb3BhZ2F0aW9uKCk7XG4gICAgICAgICAgICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgICAgICAgICAgICAgIHZhciAkZm9ybSA9ICQodGhpcykuY2xvc2VzdCgnZm9ybScpLFxuICAgICAgICAgICAgICAgICAgICAkaW5wdXQgPSAkZm9ybS5maW5kKCdpbnB1dFt0eXBlPVwic2VhcmNoXCJdJyksXG4gICAgICAgICAgICAgICAgICAgIGFjdGl2ZUNsYXNzID0gJ2FjdGl2ZSc7XG4gICAgICAgICAgICAgICAgaWYoJGZvcm0uaGFzQ2xhc3MoYWN0aXZlQ2xhc3MpKSB7XG4gICAgICAgICAgICAgICAgICAgICRmb3JtLnJlbW92ZUNsYXNzKGFjdGl2ZUNsYXNzKTtcbiAgICAgICAgICAgICAgICAgICAgJGlucHV0LmJsdXIoKTtcbiAgICAgICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgICAgICAkZm9ybS5hZGRDbGFzcyhhY3RpdmVDbGFzcyk7XG4gICAgICAgICAgICAgICAgICAgICRpbnB1dC5mb2N1cygpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9LFxuICAgICAgICBvcGVuTG9jYXRpb246IGZ1bmN0aW9uKCR0aGlzKSB7XG5cbiAgICAgICAgICAgICR0aGlzLmNsaWNrKGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgIHZhciAkdG1wbCA9ICQoTlNJTktBX0dsb2JhbC50bXBsKCdsb2NhdGlvbicsIHtkaXJlY3Rpb25zIDogTlNJTktBX01hcC52YXJzLndheXBvaW50c30pKTtcblxuICAgICAgICAgICAgICAgICQoJ2JvZHknKS5wcmVwZW5kKCR0bXBsKTtcblxuICAgICAgICAgICAgICAgIHZhciAkbG9jYXRpb25zX193cmFwcGVyID0gJHRtcGwuZmluZCgnI2xvY2F0aW9uc19fd3JhcHBlcicpO1xuXG4gICAgICAgICAgICAgICAgJGxvY2F0aW9uc19fd3JhcHBlci5zbGljayh7XG4gICAgICAgICAgICAgICAgICAgIGFycm93czogZmFsc2UsXG4gICAgICAgICAgICAgICAgICAgIHZhcmlhYmxlV2lkdGg6IHRydWUsXG4gICAgICAgICAgICAgICAgICAgIGluaXRpYWxTbGlkZTogJHRoaXMuY2xvc2VzdCgnbGknKS5kYXRhKCdsb2NhdGlvbi1pZCcpXG4gICAgICAgICAgICAgICAgfSk7XG5cbiAgICAgICAgICAgICAgICAvLyR0bXBsLmZpbmQoJyNsb2NhdGlvbl9jbG9zZScpLnNsaWNrKCdzbGlja1ByZXYnKTtcbiAgICAgICAgICAgICAgICAkdG1wbC5maW5kKCcjbG9jYXRpb25fcHJldiwjbG9jYXRpb25fbmV4dCcpLmNsaWNrKGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICAgICBpZih0aGlzLmlkID09ICdsb2NhdGlvbl9uZXh0Jykge1xuICAgICAgICAgICAgICAgICAgICAgICAgJGxvY2F0aW9uc19fd3JhcHBlci5zbGljaygnc2xpY2tOZXh0Jyk7XG4gICAgICAgICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAkbG9jYXRpb25zX193cmFwcGVyLnNsaWNrKCdzbGlja1ByZXYnKTtcbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgICAgICAgJHRtcGwuZmluZCgnI2xvY2F0aW9uX2Nsb3NlJykuY2xpY2soZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgICAgICR0bXBsLnJlbW92ZUNsYXNzKCdhY3RpdmUnKTtcbiAgICAgICAgICAgICAgICAgICAgJHRtcGwub25lKHRyYW5zaXRpb25FbmRUcmlnZ2VyLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICR0bXBsLnJlbW92ZSgpO1xuICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICB9KVxuXG4gICAgICAgICAgICAgICAgc2V0VGltZW91dChmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAgICAgJHRtcGwuYWRkQ2xhc3MoJ2FjdGl2ZScpO1xuICAgICAgICAgICAgICAgIH0sMTAwKTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9LFxuICAgICAgICBzZWxlY3RSb3V0ZTogZnVuY3Rpb24oJHRoaXMpIHtcbiAgICAgICAgICAgICR0aGlzLmNoYW5nZShmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICBOU0lOS0FfTWFwLmluaXRSb3V0ZSgkKHRoaXMpLnZhbCgpKTtcbiAgICAgICAgICAgICAgICBOU0lOS0FfTWFwLmdlblJvdXRlKCk7XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfSxcbiAgICAgICAgcXVpY2tsaW5rczogZnVuY3Rpb24oJHRoaXMpIHtcbiAgICAgICAgICAgIHZhciAkaGVhZGVyID0gJHRoaXMuZmluZCgnbGF5ZXItY29udGVudC1oZWFkZXInKSxcbiAgICAgICAgICAgICAgICBoZWFkZXJTdGFuZGFyZFZhbHVlID0gJGhlYWRlci50ZXh0KCk7XG4gICAgICAgICAgICAkdGhpcy5vbignbW92ZVRyYW5zaXRpb25TdGF0dXMnLCBmdW5jdGlvbihlLCBwZXJjZW50YWdlKSB7XG4gICAgICAgICAgICAgICAgaWYocGVyY2VudGFnZT4wKSB7XG4gICAgICAgICAgICAgICAgICAgICRoZWFkZXIudGV4dChoZWFkZXJTdGFuZGFyZFZhbHVlKTtcbiAgICAgICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgICAgICAkaGVhZGVyLnRleHQoJ1N0YWR0cnVuZGdhbmcnKTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgfVxuXG4gICAgfVxufVxuIiwiLyoqXG4qIERlc2NyaXB0aW9uIGZvciBOU0lOS0FfQWN0aW9uc1xuKiBAcHJpdmF0ZVxuKiBAcHJvcGVydHkgTlNJTktBX0FjdGlvbnNcbiovXG52YXIgTlNJTktBX2pRdWVyeUV4dGVuc2lvbnMgPSB7XG4gICAgdmFyczoge1xuICAgIH0sXG4gICAgaW5pdDogZnVuY3Rpb24oKSB7XG4gICAgICAgIGpRdWVyeS5mbi5leHRlbmQodGhpcy5mdW5jdGlvbnMpO1xuICAgIH0sXG4gICAgZnVuY3Rpb25zOiB7XG4gICAgICAgIGNsaWNrdG91Y2g6IGZ1bmN0aW9uIChmbikge1xuICAgICAgICAgICAgcmV0dXJuICQodGhpcykub24oJ2NsaWNrJyxmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAkKHRoaXMpLnRyaWdnZXIoJ3RvdWNoc3RhcnQnLFt0cnVlXSk7XG4gICAgICAgICAgICB9KS5vbignbW91c2Vkb3duIG1vdXNlbW92ZSBtb3VzZXVwJywgZnVuY3Rpb24oZSkge1xuICAgICAgICAgICAgICAgIGUucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICAgICAgICBlLnN0b3BJbW1lZGlhdGVQcm9wYWdhdGlvbigpO1xuICAgICAgICAgICAgICAgIGUuc3RvcFByb3BhZ2F0aW9uKCk7XG4gICAgICAgICAgICB9KS5vbigndG91Y2hzdGFydCcsIGZuKTtcbiAgICAgICAgfVxuXG4gICAgfVxufVxuIiwiLyoqXG4qIERlc2NyaXB0aW9uIGZvciBOU0lOS0FfTGF5ZXJzXG4qIEBwcml2YXRlXG4qIEBwcm9wZXJ0eSBOU0lOS0FfTGF5ZXJzXG4qL1xudmFyIE5TSU5LQV9MYXllcnMgPSB7XG4gICAgdmFyczoge1xuICAgICAgICBzbmFwUG9pbnQ6IDUwXG4gICAgfSxcbiAgICBpbml0OiBmdW5jdGlvbigpIHtcbiAgICAgICAgdGhpcy5pbml0SHRtbExheWVyVGFnKCk7XG4gICAgICAgICQoJ2xheWVyJykuZWFjaCh0aGlzLnNpbmdsZSk7XG4gICAgICAgIHRoaXMubGF5ZXJMaW5rcygpO1xuICAgICAgICB0aGlzLmlkZW50aWZ5TGF5ZXJzKCk7XG5cbiAgICAgICAgLyogU2FmYXJpIGlPUyBicm93c2VyaGFjayBpbmhlcml0IG1pbi1oZWlnaHQqL1xuICAgICAgICB2YXIgJGNvbnRlbnQgPSAkKCdtYWluPi5jb250ZW50Jyk7XG4gICAgICAgIG5ldyBSZXNpemVTZW5zb3IoJGNvbnRlbnRbMF0sIGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgJGNvbnRlbnQudHJpZ2dlcignYnJvd3NlcmhhY2tfaW5oZXJpdF9oZWlnaHQnKTtcbiAgICAgICAgfSk7XG5cbiAgICAgICAgdmFyICRhcnRpY2xlID0gJCgnbGF5ZXI+YXJ0aWNsZScpO1xuICAgICAgICAkKCdsYXllcj5hcnRpY2xlJykuZWFjaChmdW5jdGlvbigpIHtcbiAgICAgICAgICAgIHZhciAkdGhpcyA9ICQodGhpcyk7XG4gICAgICAgICAgICBuZXcgUmVzaXplU2Vuc29yKCR0aGlzLnBhcmVudCgpWzBdLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAkdGhpcy50cmlnZ2VyKCdfcmVzaXplJyk7XG4gICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgJHRoaXMub24oJ19yZXNpemUnLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAkdGhpcy5jc3MoeyAnbWluLWhlaWdodCcgOiAkKHdpbmRvdykuaGVpZ2h0KCkgLSAkdGhpcy5vZmZzZXQoKS50b3AgfSk7XG4gICAgICAgICAgICB9KS50cmlnZ2VyKCdfcmVzaXplJyk7XG4gICAgICAgIH0pO1xuXG4gICAgICAgICRjb250ZW50Lm9uKCdicm93c2VyaGFja19pbmhlcml0X2hlaWdodCcsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgJCh0aGlzKS5jc3MoJ2hlaWdodCcsICcnKS5jc3MoJ2hlaWdodCcsIHRoaXMuY2xpZW50SGVpZ2h0KTtcbiAgICAgICAgfSkudHJpZ2dlcignYnJvd3NlcmhhY2tfaW5oZXJpdF9oZWlnaHQnKTtcblxuICAgIH0sXG4gICAgbGF5ZXJMaW5rczogZnVuY3Rpb24oJGJvZHkpIHtcbiAgICAgICAgJGJvZHkgPSAkYm9keSB8fCAkKCdib2R5Jyk7XG4gICAgICAgICQoJ2FbaHJlZl0nKS5jbGljayhmdW5jdGlvbihlKSB7IGUucHJldmVudERlZmF1bHQoKTsgfSkuY2xpY2t0b3VjaChmdW5jdGlvbihlKSB7XG4gICAgICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgICAgICBOU0lOS0FfR2xvYmFsLmFqYXhQYWdlKHRoaXMuaHJlZiwgTlNJTktBX0xheWVycy5wcmVwYXJlUmVjZWl2ZXJMYXllcik7XG4gICAgICAgIH0pO1xuICAgIH0sXG4gICAgaWRlbnRpZnlMYXllcnM6IGZ1bmN0aW9uKCRib2R5KSB7XG4gICAgICAgICRib2R5ID0gJGJvZHkgfHwgJCgnYm9keScpO1xuICAgICAgICAkKCdsYXllcicsICRib2R5KS5lYWNoKGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgdGhpcy5kYXRhc2V0LmlkID0gJC5tYXAoJCh0aGlzKS5wYXJlbnRzKCkuZmlsdGVyKCdtYWluLGxheWVyJyksIGZ1bmN0aW9uKGl0ZW0pIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gaXRlbS5ub2RlTmFtZSsoaXRlbS5pZD8nIycraXRlbS5pZDonJyk7XG4gICAgICAgICAgICB9KS5yZXZlcnNlKCkuam9pbignPicpKyc+Jyt0aGlzLm5vZGVOYW1lKyh0aGlzLmlkPycjJyt0aGlzLmlkOicnKTtcbiAgICAgICAgfSk7XG4gICAgfSxcbiAgICBwcmVwYXJlUmVjZWl2ZXJMYXllcjogZnVuY3Rpb24oJGRhdGEpIHtcbiAgICAgICAgJG1haW4gPSAkKCRkYXRhLmZpbHRlcihmdW5jdGlvbihpdGVtKSB7XG4gICAgICAgICAgICBpZihpdGVtLm5vZGVOYW1lID09ICdNQUlOJykge1xuICAgICAgICAgICAgICAgIHJldHVybiBpdGVtO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICB9XG4gICAgICAgIH0pWzBdKTtcblxuICAgICAgICBOU0lOS0FfTGF5ZXJzLmlkZW50aWZ5TGF5ZXJzKCRtYWluKTtcblxuICAgICAgICAkKCdsYXllcicpLmVhY2goZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICB2YXIgJHQgPSAkKHRoaXMpLFxuICAgICAgICAgICAgICAgICRhamF4U2libGluZyA9ICRtYWluLmZpbmQoJ2xheWVyW2RhdGEtaWQ9XCInKyQodGhpcykuZGF0YSgnaWQnKSsnXCJdJyk7XG5cbiAgICAgICAgICAgIGlmKCRhamF4U2libGluZy5sZW5ndGgpIHtcbiAgICAgICAgICAgICAgICB2YXIgJHRDaGlsZExheWVyID0gJCh0aGlzKS5maW5kKCc+bGF5ZXInKSxcbiAgICAgICAgICAgICAgICAgICAgJHNDaGlsZExheWVyID0gJGFqYXhTaWJsaW5nLmZpbmQoJz5sYXllcicpO1xuICAgICAgICAgICAgICAgIGlmKCRzQ2hpbGRMYXllci5sZW5ndGggJiYgISR0Q2hpbGRMYXllci5sZW5ndGgpIHtcbiAgICAgICAgICAgICAgICAgICAgJHRbMF0uYXBwZW5kTGF5ZXIoJHNDaGlsZExheWVyKTtcbiAgICAgICAgICAgICAgICB9IGVsc2UgaWYoJHRDaGlsZExheWVyLmxlbmd0aCAmJiAoJHRDaGlsZExheWVyLmRhdGEoJ2lkJykgIT0gJHNDaGlsZExheWVyLmRhdGEoJ2lkJykpKSB7XG4gICAgICAgICAgICAgICAgICAgICR0Q2hpbGRMYXllclswXS5yZW1vdmVMYXllcihmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICR0WzBdLmFwcGVuZExheWVyKCRzQ2hpbGRMYXllcik7XG4gICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgJCh0aGlzKS50cmlnZ2VyKCdyZW1vdmVMYXllcicpO1xuICAgICAgICAgICAgfVxuICAgICAgICB9KTtcblxuICAgICAgICAkbGF5ZXJzID0gJCgkbWFpblswXSkuZmluZCgnbGF5ZXInKTtcbiAgICB9LFxuICAgIGluaXRIdG1sTGF5ZXJUYWc6IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgbGF5ZXJUYWcgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KCdsYXllcicpO1xuICAgICAgICB2YXIgbGF5ZXJDb250ZW50VGFnID0gZG9jdW1lbnQuY3JlYXRlRWxlbWVudCgnbGF5ZXItY29udGVudCcpO1xuICAgIH0sXG4gICAgc2luZ2xlOiBmdW5jdGlvbigpIHtcbiAgICAgICAgdGhpcy52YXJzID0ge1xuICAgICAgICAgICAgJGxheWVyQ29udGVudDogJCh0aGlzKS5maW5kKCc+bGF5ZXItY29udGVudCcpLFxuICAgICAgICAgICAgJGxheWVyQ29udGVudEhlaWdodEluZGljYXRvcjogJCgnPGxheWVyLWhlaWdodC1pbmRpY2F0b3IgLz4nKVxuICAgICAgICB9O1xuXG4gICAgICAgIGlmKCQodGhpcykuYXR0cignZGlzYWJsZWQnKSkge1xuICAgICAgICAgICAgcmV0dXJuIHRoaXM7XG4gICAgICAgIH1cbiAgICAgICAgdGhpcy52YXJzLiRsYXllckNvbnRlbnQuYXBwZW5kKCB0aGlzLnZhcnMuJGxheWVyQ29udGVudEhlaWdodEluZGljYXRvcik7XG4gICAgICAgIHZhciAkbGF5ZXJDb250ZW50SGVhZGVyID0gJCh0aGlzKS5maW5kKCdsYXllci1jb250ZW50LWhlYWRlcicpO1xuICAgICAgICB0aGlzLnZhcnMuJGhhbmRsZSA9ICRsYXllckNvbnRlbnRIZWFkZXIubGVuZ3RoID8gJGxheWVyQ29udGVudEhlYWRlciA6IHRoaXMudmFycy4kbGF5ZXJDb250ZW50O1xuXG4gICAgICAgIGlmKGhhbmRsZSA9ICQodGhpcykuYXR0cignaGFuZGxlJykpIHtcbiAgICAgICAgICAgIHRoaXMudmFycy4kaGFuZGxlID0gJChoYW5kbGUsIHRoaXMpO1xuICAgICAgICB9XG5cbiAgICAgICAgdGhpcy52YXJzLiRoYW5kbGVbMF0ubWluSGVpZ2h0ID0gcGFyc2VGbG9hdCh0aGlzLnZhcnMuJGxheWVyQ29udGVudC5jc3MoJ21pbi1oZWlnaHQnKSk7XG4gICAgICAgIHRoaXMudmFycy4kaGFuZGxlWzBdLm1heEhlaWdodCA9IHRoaXMudmFycy4kbGF5ZXJDb250ZW50SGVpZ2h0SW5kaWNhdG9yWzBdLm9mZnNldFRvcDtcblxuICAgICAgICB0aGlzLnZhcnMuJGhhbmRsZVswXS5vcGVuZWQgPSAoJCh0aGlzKS5hdHRyKCdvcGVuZWQnKT09J3RydWUnKSB8fCBmYWxzZTtcblxuICAgICAgICB0aGlzLnZhcnMuJGhhbmRsZVswXS5wcmV2RGlyZWN0aW9uID0gdGhpcy52YXJzLiRoYW5kbGVbMF0ub3BlbmVkID8gLTEgOiAxO1xuXG4gICAgICAgIHRoaXMudmFycy4kaGFuZGxlWzBdLiRsYXllciA9ICQodGhpcyk7XG4gICAgICAgIHRoaXMudmFycy4kaGFuZGxlWzBdLiRsYXllckNvbnRlbnQgPSB0aGlzLnZhcnMuJGxheWVyQ29udGVudDtcblxuICAgICAgICB0aGlzLnZhcnMuJGxheWVyQ29udGVudC5jc3Moe1xuICAgICAgICAgICAgaGVpZ2h0OiB0aGlzLnZhcnMuJGxheWVyQ29udGVudFswXS5vcGVuZWQ/dGhpcy52YXJzLiRsYXllckNvbnRlbnQuaGVpZ2h0KCk6dGhpcy52YXJzLiRsYXllckNvbnRlbnRbMF0ubWluSGVpZ2h0XG4gICAgICAgIH0pO1xuXG5cbiAgICAgICAgdGhpcy52YXJzLiRoYW5kbGVbMF0ud2hpbGVPcGVuQ2xvc2UgPVxuICAgICAgICAgICAgKCQodGhpcykuYXR0cignd2hpbGVPcGVuQ2xvc2UnKT8kLm1hcCgkKHRoaXMpLmF0dHIoJ3doaWxlT3BlbkNsb3NlJykuc3BsaXQoJywnKSwgZnVuY3Rpb24oaWQpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gJCgnIycraWQpO1xuICAgICAgICAgICAgfSk6ZmFsc2UpO1xuXG4gICAgICAgIHZhciBfdGhpcyA9IHRoaXM7XG5cbiAgICAgICAgdGhpcy5ldmVudEhhbmRsZXIgPSB7XG4gICAgICAgICAgICB0b3VjaHN0YXJ0OiBmdW5jdGlvbihlLCBwcmV2ZW50VGhpc1Byb3BhZ2F0aW9uKSB7XG4gICAgICAgICAgICAgICAgLy9lLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgICAgICAgICAgZS5zdG9wSW1tZWRpYXRlUHJvcGFnYXRpb24oKTtcblxuICAgICAgICAgICAgICAgICQodGhpcykudHJpZ2dlcignbW92ZVRyYW5zaXRpb25TdGF0dXMnLCAoKCQodGhpcykuaGVpZ2h0KCkvdGhpcy5tYXhIZWlnaHQpKjEwMCkpO1xuXG4gICAgICAgICAgICAgICAgdGhpcy5tYXhIZWlnaHQgPSB0aGlzLiRsYXllclswXS52YXJzLiRsYXllckNvbnRlbnRIZWlnaHRJbmRpY2F0b3JbMF0ub2Zmc2V0VG9wO1xuXG4gICAgICAgICAgICAgICAgaWYocHJldmVudFRoaXNQcm9wYWdhdGlvbikge1xuICAgICAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgICAgJCh0aGlzKS5hZGRDbGFzcygndG91Y2hzdGFydCcpO1xuXG4gICAgICAgICAgICAgICAgaWYodHlwZW9mIGUuZGV0YWlsICE9ICd1bmRlZmluZWQnKSB7XG4gICAgICAgICAgICAgICAgICAgIGlmKHR5cGVvZiBlLmRldGFpbC5jaGFuZ2VkVG91Y2hlcyAhPSAndW5kZWZpbmVkJykge1xuICAgICAgICAgICAgICAgICAgICAgICAgZS5jaGFuZ2VkVG91Y2hlcyA9IGUuZGV0YWlsLmNoYW5nZWRUb3VjaGVzO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfVxuXG5cbiAgICAgICAgICAgICAgICB0aGlzLnByZXZDbGllbnRZID0gdGhpcy50b3VjaHN0YXJ0UG9zaXRpb25ZID0gKGUuY2hhbmdlZFRvdWNoZXMgPyAoZS5jaGFuZ2VkVG91Y2hlc1swXS5jbGllbnRZLXRoaXMub2Zmc2V0VG9wKSA6IDApO1xuICAgICAgICAgICAgICAgIHRoaXMudG91Y2hzdGFydEhlaWdodCA9ICQodGhpcykuaGVpZ2h0KCk7XG4gICAgICAgICAgICAgICAgdGhpcy5yZXNpc3RhbmNlID0gMS4xO1xuXG4gICAgICAgICAgICAgICAgICAgICAgICBjb25zb2xlLmxvZygndG91Y2hzdGFydFBvc2l0aW9uWScsIHRoaXMudG91Y2hzdGFydFBvc2l0aW9uWSk7XG5cbiAgICAgICAgICAgICAgICB0aGlzLmRpcmVjdGlvbiA9IHRoaXMub3BlbmVkPy0xOjE7XG5cbiAgICAgICAgICAgICAgICBpZighdGhpcy5vcGVuZWQpIHtcbiAgICAgICAgICAgICAgICAgICAgJC5lYWNoKHRoaXMud2hpbGVPcGVuQ2xvc2UsIGZ1bmN0aW9uKGksICRpdGVtKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAkaXRlbS50cmlnZ2VyKCdjbG9zZUxheWVyJyk7XG4gICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAkKHRoaXMpLnJlbW92ZUNsYXNzKCdvcGVuJyk7XG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgdG91Y2htb3ZlOiBmdW5jdGlvbihlKSB7XG4gICAgICAgICAgICAgICAgZS5zdG9wSW1tZWRpYXRlUHJvcGFnYXRpb24oKTtcbiAgICAgICAgICAgICAgICAkKHRoaXMpLnJlbW92ZUNsYXNzKCd0b3VjaHN0YXJ0JykuYWRkQ2xhc3MoJ3RvdWNobW92ZScpO1xuXG4gICAgICAgICAgICAgICAgaWYodHlwZW9mIGUuZGV0YWlsICE9ICd1bmRlZmluZWQnKSB7XG4gICAgICAgICAgICAgICAgICAgIGlmKHR5cGVvZiBlLmRldGFpbC5jaGFuZ2VkVG91Y2hlcyAhPSAndW5kZWZpbmVkJykge1xuICAgICAgICAgICAgICAgICAgICAgICAgZS5jaGFuZ2VkVG91Y2hlcyA9IGUuZGV0YWlsLmNoYW5nZWRUb3VjaGVzO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgICAgdmFyIGN1cnJlbnRDbGllbnRZID0gZS5jaGFuZ2VkVG91Y2hlc1swXS5jbGllbnRZLXRoaXMub2Zmc2V0VG9wLFxuICAgICAgICAgICAgICAgICAgICBjYWxjID0gY3VycmVudENsaWVudFktdGhpcy50b3VjaHN0YXJ0UG9zaXRpb25ZLFxuICAgICAgICAgICAgICAgICAgICBjYWxjID0gdGhpcy50b3VjaHN0YXJ0SGVpZ2h0K2NhbGM7XG5cbiAgICAgICAgICAgICAgICB0aGlzLmRpcmVjdGlvbiA9IHRoaXMucHJldkNsaWVudFkgPiBjdXJyZW50Q2xpZW50WSA/IC0xIDogMTtcblxuICAgICAgICAgICAgICAgIC8qXG4gICAgICAgICAgICAgICAgaWYodGhpcy5kaXJlY3Rpb24gIT0gdGhpcy5wcmV2RGlyZWN0aW9uKSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbnNvbGUubG9nKHRoaXMuZGlyZWN0aW9uLCB0aGlzLnByZXZEaXJlY3Rpb24pO1xuICAgICAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICAgICAgfSovXG5cbiAgICAgICAgICAgICAgICBpZih0aGlzLiRsYXllci5hdHRyKCdpbnZlcnRUb3VjaCcpID09ICd0cnVlJykge1xuICAgICAgICAgICAgICAgICAgICB0aGlzLmRpcmVjdGlvbiA9IHRoaXMuZGlyZWN0aW9uKi0xO1xuICAgICAgICAgICAgICAgICAgICBjYWxjID0gKGNhbGMtdGhpcy50b3VjaHN0YXJ0SGVpZ2h0KSotMTtcbiAgICAgICAgICAgICAgICB9XG5cbiAgICAgICAgICAgICAgICAkKHRoaXMpLnRyaWdnZXIoJ21vdmVUcmFuc2l0aW9uU3RhdHVzJywgKChjYWxjL3RoaXMubWF4SGVpZ2h0KSoxMDApKTtcblxuICAgICAgICAgICAgICAgIHZhciB0YXJnZXRQb3NpdGlvbiA9IHRoaXMuZGlyZWN0aW9uID09PSAxID8gdGhpcy5tYXhIZWlnaHQgOiB0aGlzLm1pbkhlaWdodDtcblxuICAgICAgICAgICAgICAgIGlmKFxuICAgICAgICAgICAgICAgICAgICAodGhpcy5kaXJlY3Rpb24gPT09IDEgJiYgY2FsYyA+PSB0aGlzLm1heEhlaWdodCApIHx8XG4gICAgICAgICAgICAgICAgICAgICh0aGlzLmRpcmVjdGlvbiA9PT0gLTEgJiYgY2FsYyA8PSB0aGlzLm1pbkhlaWdodCApXG4gICAgICAgICAgICAgICAgKSB7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICAgICAgICB9XG5cbiAgICAgICAgICAgICAgICB0aGlzLiRsYXllckNvbnRlbnQuY3NzKHtcbiAgICAgICAgICAgICAgICAgICAgaGVpZ2h0OiAoY2FsYylcbiAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgICAgIHRoaXMucmVzaXN0YW5jZSA9IHRoaXMucmVzaXN0YW5jZSouMTtcbiAgICAgICAgICAgICAgICB0aGlzLnByZXZDbGllbnRZID0gY3VycmVudENsaWVudFk7XG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgdG91Y2hlbmQ6IGZ1bmN0aW9uKGUpIHtcbiAgICAgICAgICAgICAgICAvL2UucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICAgICAgICAvKlxuICAgICAgICAgICAgICAgIGlmKHRoaXMuZGlyZWN0aW9uICE9IHRoaXMucHJldkRpcmVjdGlvbikge1xuICAgICAgICAgICAgICAgICAgICB0aGlzLnByZXZEaXJlY3Rpb24gPSB0aGlzLmRpcmVjdGlvbjtcbiAgICAgICAgICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgICAgICAgIH0qL1xuXG4gICAgICAgICAgICAgICAgdGhpcy5wcmV2RGlyZWN0aW9uID0gdGhpcy5kaXJlY3Rpb247XG5cbiAgICAgICAgICAgICAgICBlLnN0b3BJbW1lZGlhdGVQcm9wYWdhdGlvbigpO1xuICAgICAgICAgICAgICAgIHRoaXMuJGxheWVyQ29udGVudC5yZW1vdmVDbGFzcygndG91Y2hzdGFydCcpLnJlbW92ZUNsYXNzKCd0b3VjaG1vdmUnKS5hZGRDbGFzcygndG91Y2hlbmQnKTtcbiAgICAgICAgICAgICAgICB0aGlzLnRvdWNoc3RhcnRQb3NpdGlvblkgPSAwO1xuXG4gICAgICAgICAgICAgICAgaWYodGhpcy5kaXJlY3Rpb24gPT09IDEpIHtcbiAgICAgICAgICAgICAgICAgICAgdGhpcy4kbGF5ZXJDb250ZW50LnBhcmVudCgpLnRyaWdnZXIoJ2xheWVyLW9wZW5lZCcpO1xuICAgICAgICAgICAgICAgICAgICB2YXIgZmluYWxIZWlnaHQgPSB0aGlzLm1heEhlaWdodDtcbiAgICAgICAgICAgICAgICAgICAgdGhpcy5vcGVuZWQgPSB0cnVlO1xuICAgICAgICAgICAgICAgICAgICB0aGlzLiRsYXllckNvbnRlbnQudHJpZ2dlcignbW92ZVRyYW5zaXRpb25TdGF0dXMnLCAxMDApO1xuICAgICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgIHRoaXMuJGxheWVyQ29udGVudC5wYXJlbnQoKS50cmlnZ2VyKCdsYXllci1jbG9zZWQnKTtcbiAgICAgICAgICAgICAgICAgICAgdmFyIGZpbmFsSGVpZ2h0ID0gdGhpcy5taW5IZWlnaHQ7XG4gICAgICAgICAgICAgICAgICAgIHRoaXMub3BlbmVkID0gZmFsc2U7XG4gICAgICAgICAgICAgICAgICAgIHRoaXMuJGxheWVyQ29udGVudC50cmlnZ2VyKCdtb3ZlVHJhbnNpdGlvblN0YXR1cycsIDApO1xuICAgICAgICAgICAgICAgIH1cblxuICAgICAgICAgICAgICAgIHRoaXMuJGxheWVyQ29udGVudC5jc3Moe1xuICAgICAgICAgICAgICAgICAgICBoZWlnaHQ6IGZpbmFsSGVpZ2h0XG4gICAgICAgICAgICAgICAgfSkub25lKHRyYW5zaXRpb25FbmRUcmlnZ2VyLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAgICAgJCh0aGlzKS5yZW1vdmVDbGFzcygndG91Y2hlbmQnKTtcbiAgICAgICAgICAgICAgICAgICAgaWYoX3RoaXMudmFycy4kaGFuZGxlWzBdLm9wZW5lZCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgJCh0aGlzKS5hZGRDbGFzcygnb3BlbicpO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICQod2luZG93KS50cmlnZ2VyKCdsYXllckNoYW5nZScpO1xuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICQod2luZG93KS50cmlnZ2VyKCdsYXllckNoYW5nZScpO1xuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHRvdWNoU2ltdWxhdGlvbjogZnVuY3Rpb24oZSkge1xuICAgICAgICAgICAgICAgIHZhciB0eXBlID0gZmFsc2U7XG5cbiAgICAgICAgICAgICAgICBzd2l0Y2ggKGUudHlwZSlcbiAgICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgICAgIGNhc2UgXCJtb3VzZWRvd25cIjpcbiAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMuaXNNb3VzZURvd24gPSB0cnVlO1xuICAgICAgICAgICAgICAgICAgICAgICAgdHlwZSA9IFwidG91Y2hzdGFydFwiO1xuICAgICAgICAgICAgICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgICAgICAgICAgIGNhc2UgXCJtb3VzZW1vdmVcIjpcbiAgICAgICAgICAgICAgICAgICAgICAgIGlmKHRoaXMuaXNNb3VzZURvd24pIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB0eXBlID0gXCJ0b3VjaG1vdmVcIjtcbiAgICAgICAgICAgICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgdHlwZSA9IGZhbHNlO1xuICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgICAgICAgICAgIGNhc2UgXCJtb3VzZXVwXCI6XG4gICAgICAgICAgICAgICAgICAgICAgICB0aGlzLmlzTW91c2VEb3duID0gZmFsc2U7XG4gICAgICAgICAgICAgICAgICAgICAgICB0eXBlID0gXCJ0b3VjaGVuZFwiO1xuICAgICAgICAgICAgICAgICAgICAgICAgaWYodGhpcyA9PSB3aW5kb3cpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB3aW5kb3cuY3VycmVudFRvdWNoZWRFbGVtLmRpc3BhdGNoRXZlbnQobmV3IEV2ZW50KCd0b3VjaGVuZCcpKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAvLyQod2luZG93LmN1cnJlbnRUb3VjaGVkRWxlbSkudHJpZ2dlcigndG91Y2hlbmQnKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgICAgICAgICAgICBkZWZhdWx0OiByZXR1cm47XG4gICAgICAgICAgICAgICAgfVxuXG5cblxuICAgICAgICAgICAgICAgIGlmKHR5cGUpIHtcbiAgICAgICAgICAgICAgICAgICAgd2luZG93LmN1cnJlbnRUb3VjaGVkRWxlbSA9IHRoaXM7XG4gICAgICAgICAgICAgICAgICAgIHZhciBzaW11bGF0ZWRFdmVudCA9IG5ldyBDdXN0b21FdmVudCh0eXBlLCB7XG4gICAgICAgICAgICAgICAgICAgICAgICBmb286ICdkZXYnLFxuICAgICAgICAgICAgICAgICAgICAgICAgZGV0YWlsOiB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgY2hhbmdlZFRvdWNoZXM6IFt7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNsaWVudFggOiBlLmNsaWVudFgsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNsaWVudFkgOiBlLmNsaWVudFlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9XVxuICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICB9KTtcblxuXG4gICAgICAgICAgICAgICAgICAgIHNpbXVsYXRlZEV2ZW50LnRlc3QgPSAndGVzdCc7XG5cbiAgICAgICAgICAgICAgICAgICAgdGhpcy5kaXNwYXRjaEV2ZW50KHNpbXVsYXRlZEV2ZW50KTtcbiAgICAgICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICB9XG5cbiAgICAgICAgaWYodGhpcy52YXJzLiRsYXllckNvbnRlbnRbMF0ubWluSGVpZ2h0ID49IHRoaXMudmFycy4kbGF5ZXJDb250ZW50WzBdLm1heEhlaWdodCkge1xuICAgICAgICAgICAgdGhpcy52YXJzLiRsYXllckNvbnRlbnRbMF0uZGlzcGF0Y2hFdmVudChuZXcgRXZlbnQoJ3RvdWNoZW5kJykpO1xuICAgICAgICAgICAgLy90aGlzLnZhcnMuJGxheWVyQ29udGVudC50cmlnZ2VyKCd0b3VjaGVuZCcsIHRoaXMuZXZlbnRIYW5kbGVyLnRvdWNoZW5kKTtcbiAgICAgICAgICAgIHJldHVybiB0aGlzO1xuICAgICAgICB9XG5cblxuICAgICAgICB0aGlzLnZhcnMuJGhhbmRsZVswXS5hZGRFdmVudExpc3RlbmVyKCd0b3VjaHN0YXJ0JywgdGhpcy5ldmVudEhhbmRsZXIudG91Y2hzdGFydCwgeyBwYXNzaXZlIDogdHJ1ZSB9KTtcbiAgICAgICAgdGhpcy52YXJzLiRoYW5kbGVbMF0uYWRkRXZlbnRMaXN0ZW5lcigndG91Y2htb3ZlJywgdGhpcy5ldmVudEhhbmRsZXIudG91Y2htb3ZlLCB7IHBhc3NpdmUgOiB0cnVlIH0pO1xuICAgICAgICB0aGlzLnZhcnMuJGhhbmRsZVswXS5hZGRFdmVudExpc3RlbmVyKCd0b3VjaGVuZCcsIHRoaXMuZXZlbnRIYW5kbGVyLnRvdWNoZW5kLCB7IHBhc3NpdmUgOiB0cnVlIH0pO1xuICAgICAgICB0aGlzLnZhcnMuJGhhbmRsZVswXS5hZGRFdmVudExpc3RlbmVyKCd0b3VjaGNhbmNlbCcsIHRoaXMuZXZlbnRIYW5kbGVyLnRvdWNoZW5kLCB7IHBhc3NpdmUgOiB0cnVlIH0pO1xuXG4gICAgICAgIGlmKCFOU0lOS0FfR2xvYmFsLmlzX21vYmlsZSgpKSB7XG4gICAgICAgICAgICB0aGlzLnZhcnMuJGhhbmRsZVswXS5pc01vdXNlRG93biA9IGZhbHNlO1xuICAgICAgICAgICAgLyp0aGlzLnZhcnMuJGhhbmRsZS5vbihcbiAgICAgICAgICAgICAgICAnbW91c2Vkb3duIG1vdXNlbW92ZSBtb3VzZXVwJyxcbiAgICAgICAgICAgICAgICB0aGlzLmV2ZW50SGFuZGxlci50b3VjaFNpbXVsYXRpb25cbiAgICAgICAgICAgICk7Ki9cblxuICAgICAgICAgICAgJC5lYWNoKFsnbW91c2Vkb3duJywgJ21vdXNlbW92ZScsICdtb3VzZXVwJ10sIGZ1bmN0aW9uKGksIGV2ZW50VHlwZSkge1xuICAgICAgICAgICAgICAgIF90aGlzLnZhcnMuJGhhbmRsZVswXS5hZGRFdmVudExpc3RlbmVyKGV2ZW50VHlwZSwgX3RoaXMuZXZlbnRIYW5kbGVyLnRvdWNoU2ltdWxhdGlvbiwgeyBwYXNzaXZlIDogdHJ1ZSB9KTtcbiAgICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgICB3aW5kb3cuZGlzcGF0Y2hFdmVudChuZXcgRXZlbnQoJ21vdXNldXAnKSk7XG4gICAgICAgICAgICAvLyQod2luZG93KS5vbignbW91c2V1cCcsIHRoaXMuZXZlbnRIYW5kbGVyLnRvdWNoU2ltdWxhdGlvbik7XG4gICAgICAgIH1cblxuICAgICAgICAkKHRoaXMpLm9uKCdjbG9zZUxheWVyJywgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICBpZih0aGlzLnZhcnMuJGhhbmRsZVswXS5vcGVuZWQpIHtcbiAgICAgICAgICAgICAgICB0aGlzLnZhcnMuJGhhbmRsZVswXS5kaXNwYXRjaEV2ZW50KG5ldyBFdmVudCgndG91Y2hzdGFydCcpKTtcbiAgICAgICAgICAgICAgICB0aGlzLnZhcnMuJGhhbmRsZVswXS5kaXNwYXRjaEV2ZW50KG5ldyBFdmVudCgndG91Y2hlbmQnKSk7XG4gICAgICAgICAgICAgICAgLy90aGlzLnZhcnMuJGxheWVyQ29udGVudC50cmlnZ2VyKCd0b3VjaHN0YXJ0JykudHJpZ2dlcigndG91Y2hlbmQnKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgfSk7XG5cbiAgICAgICAgJCh0aGlzKS5vbignb3BlbkxheWVyJywgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICBpZighdGhpcy52YXJzLiRoYW5kbGVbMF0ub3BlbmVkKSB7XG4gICAgICAgICAgICAgICAgdGhpcy52YXJzLiRoYW5kbGVbMF0uZGlzcGF0Y2hFdmVudChuZXcgRXZlbnQoJ3RvdWNoc3RhcnQnKSk7XG4gICAgICAgICAgICAgICAgdGhpcy52YXJzLiRoYW5kbGVbMF0uZGlzcGF0Y2hFdmVudChuZXcgRXZlbnQoJ3RvdWNoZW5kJykpO1xuICAgICAgICAgICAgfVxuICAgICAgICB9KTtcblxuICAgICAgICB0aGlzLnJlbW92ZUxheWVyID0gZnVuY3Rpb24oYWZ0ZXJGbikge1xuICAgICAgICAgICAgJCh0aGlzKS5yZW1vdmUoKTtcbiAgICAgICAgICAgIGlmKHR5cGVvZiBhZnRlckZuICE9ICd1bmRlZmluZWQnKSB7XG4gICAgICAgICAgICAgICAgYWZ0ZXJGbigpO1xuICAgICAgICAgICAgfVxuICAgICAgICB9O1xuXG4gICAgICAgIHRoaXMuYXBwZW5kTGF5ZXIgPSBmdW5jdGlvbigkbGF5ZXIpIHtcbiAgICAgICAgICAgICQodGhpcykuYXBwZW5kKCRsYXllcik7XG4gICAgICAgICAgICAkbGF5ZXIuYWRkQ2xhc3MoJ2luYWN0aXZlJyk7XG5cbiAgICAgICAgICAgIE5TSU5LQV9MYXllcnMubGF5ZXJMaW5rcygkbGF5ZXIpO1xuXG4gICAgICAgICAgICAkbGF5ZXIuZWFjaChOU0lOS0FfTGF5ZXJzLnNpbmdsZSk7XG4gICAgICAgICAgICBzZXRUaW1lb3V0KGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICRsYXllci5hZGRDbGFzcygnaW5hY3RpdmUtLXRyYW5zaXRpb24nKS5yZW1vdmVDbGFzcygnaW5hY3RpdmUnKS5vbih0cmFuc2l0aW9uRW5kVHJpZ2dlciwgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgICAgIC8vJCh0aGlzKS5yZW1vdmVDbGFzcygnaW5hY3RpdmUtLXRyYW5zaXRpb24nKTtcbiAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9O1xuXG5cbiAgICB9LFxuXG59XG4iLCIvKipcbiogRGVzY3JpcHRpb24gZm9yIE5TSU5LQV9NYXBcbiogQHByaXZhdGVcbiogQHByb3BlcnR5IE5TSU5LQV9NYXBcbiovXG5cbnZhciBOU0lOS0FfTWFwID0ge1xuICAgIHZhcnM6IHtcbiAgICAgICAgdXNlckdlb1Bvc2l0aW9uOiBuZXcgQXJyYXkoKSxcbiAgICAgICAgZGlyZWN0aW9uc1NlcnZpY2U6IGZhbHNlLFxuICAgICAgICBkaXJlY3Rpb25zRGlzcGxheTogZmFsc2UsXG4gICAgICAgIGluZm9XaW5kb3c6IGZhbHNlLFxuICAgICAgICBtYXA6IGZhbHNlLFxuICAgICAgICByb3V0ZXM6IFtcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZSA6ICdSb3V0ZSBYWScsXG4gICAgICAgICAgICAgICAgcm91dGU6IFsgMSwgNSwgOCwgMTJdXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlIDogJ0VpZ2VuZSBSb3V0ZScsXG4gICAgICAgICAgICAgICAgdHlwZTogJ2N1c3RvbSdcbiAgICAgICAgICAgIH1cbiAgICAgICAgXSxcbiAgICAgICAgY3VycmVudFJvdXRlOiAwLFxuICAgICAgICB3YXlwb2ludHM6IFtcblxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnRnJlaWUgU3BpZWwtIHVuZCBTcG9ydHZlcmVpbmlndW5nLCAoRlNTVikgYW0gQWRlbmF1ZXJyaW5nJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdBZGVuYXVlcnJpbmcgMzYnLFxuICAgICAgICAgICAgICAgIGxvY2F0aW9uOiB7XG4gICAgICAgICAgICAgICAgICAgIGxhdDogNDkuMDI0MjEwLFxuICAgICAgICAgICAgICAgICAgICBsbmc6IDguMzk0NDY0XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBpbmFjdGl2ZTogdHJ1ZVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZTogJ0x1ZHdpZyBNYXJ1bSwgTHVkd2lnLU1hcnVtLVN0cmHDn2UnLFxuICAgICAgICAgICAgICAgIHN0cmVldDogJ0x1ZHdpZy1NYXJ1bS1TdHJhw59lJyxcbiAgICAgICAgICAgICAgICBsb2NhdGlvbjoge1xuICAgICAgICAgICAgICAgICAgICBsYXQ6IDQ5LjAxMzkzMSxcbiAgICAgICAgICAgICAgICAgICAgbG5nOiA4LjM2ODU0N1xuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgaW5hY3RpdmU6IHRydWVcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgdGl0bGU6ICdBZG9sZi1IaXRsZXItSGF1cyxSaXR0ZXJzdHJhw59lIDI4LzMwJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdSaXR0ZXJzdHJhw59lIDI4LzMwJyxcbiAgICAgICAgICAgICAgICBsb2NhdGlvbjoge1xuICAgICAgICAgICAgICAgICAgICBsYXQ6IDQ5LjAwNDk2OSxcbiAgICAgICAgICAgICAgICAgICAgbG5nOiA4LjM5NzkxNVxuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgaW5hY3RpdmU6IHRydWVcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgdGl0bGU6ICdBcmJlaXRlcmJpbGR1bmdzdmVyZWluJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdXaWxoZWxtc3RyYcOfZSAxNCcsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMDI5NDIsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40MDU0MDhcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnQXVndXN0LURvc2VuYmFjaC1TdHJhw59lJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdBdWd1c3QtRG9zZW5iYWNoLVN0cmHDn2UnLFxuICAgICAgICAgICAgICAgIGxvY2F0aW9uOiB7XG4gICAgICAgICAgICAgICAgICAgIGxhdDogNDkuMDAxMTMzLFxuICAgICAgICAgICAgICAgICAgICBsbmc6IDguMzQxMTIwXG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBpbmFjdGl2ZTogdHJ1ZVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZTogJ0JhZGlzY2hlcyBJbm5lbm1pbmlzdGVyaXVtJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdTY2hsb3NzcGxhdHogMTknLFxuICAgICAgICAgICAgICAgIGxvY2F0aW9uOiB7XG4gICAgICAgICAgICAgICAgICAgIGxhdDogNDkuMDEwNzcyLFxuICAgICAgICAgICAgICAgICAgICBsbmc6IDguNDAxOTg1XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBpbmFjdGl2ZTogdHJ1ZVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZTogJ0JhbmtoYXVzIFZlaXQgTC4gSG9tYnVyZ2VyJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdLYXJsc3RyYcOfZSAxMScsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OC45OTcxOTgsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC4zOTQwNDFcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnRFdNIERldXRzY2hlIFdhZmZlbi0gdW5kIE11bml0aW9uc2ZhYnJpayBBRycsXG4gICAgICAgICAgICAgICAgc3RyZWV0OiAnTG9yZW56c3RyYcOfZScsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMDE0OTksXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC4zODMwOTNcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnRsO8aHJlci1WZXJsYWcnLFxuICAgICAgICAgICAgICAgIHN0cmVldDogJ0thaXNlcnN0cmHDn2UgMTMzJyxcbiAgICAgICAgICAgICAgICBsb2NhdGlvbjoge1xuICAgICAgICAgICAgICAgICAgICBsYXQ6IDQ5LjAwOTU0MSxcbiAgICAgICAgICAgICAgICAgICAgbG5nOiA4LjQwNTIxMFxuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgaW5hY3RpdmU6IHRydWVcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgdGl0bGU6ICdHZWbDpG5nbmlzIFJpZWZzdGFobHN0cmHDn2UnLFxuICAgICAgICAgICAgICAgIHN0cmVldDogJ1JpZWZzdGFobHN0ci45JyxcbiAgICAgICAgICAgICAgICBsb2NhdGlvbjoge1xuICAgICAgICAgICAgICAgICAgICBsYXQ6IDQ5LjAxMzg3MSxcbiAgICAgICAgICAgICAgICAgICAgbG5nOiA4LjM4NTA1NlxuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgaW5hY3RpdmU6IHRydWVcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgdGl0bGU6ICdHZXN0YXBvLUhhdXB0c3RlbGxlJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdFYmVydHN0ci4gMjYnLFxuICAgICAgICAgICAgICAgIGxvY2F0aW9uOiB7XG4gICAgICAgICAgICAgICAgICAgIGxhdDogNDguOTk1MDkwLFxuICAgICAgICAgICAgICAgICAgICBsbmc6IDguMzkxODY3XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBpbmFjdGl2ZTogdHJ1ZVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZTogJ0hhdXB0YmFobmhvZicsXG4gICAgICAgICAgICAgICAgc3RyZWV0OiAnQmFobmhvZnNwbGF0eicsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMTQwNzksXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40Mjk4MzZcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnSsO8ZGlzY2hlciBGcmllZGhvZicsXG4gICAgICAgICAgICAgICAgc3RyZWV0OiAnSGFpZC0gdW5kIE5ldS1TdHIuIDQxLCA0NScsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMTQyNzksXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40MzEyNjRcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnV29obi0gdW5kIGVoZW1hbGlnZXMgR2VtZWluZGVoYXVzJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdIZXJyZW5zdHJhw59lIDE0JyxcbiAgICAgICAgICAgICAgICBsb2NhdGlvbjoge1xuICAgICAgICAgICAgICAgICAgICBsYXQ6IDQ5LjAxMDQzNixcbiAgICAgICAgICAgICAgICAgICAgbG5nOiA4LjM5OTkxMFxuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgaW5hY3RpdmU6IHRydWVcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgdGl0bGU6ICdISiB1bmQgQkRNJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdSw7xwcHVycmVyIFN0cmHDn2UnLFxuICAgICAgICAgICAgICAgIGxvY2F0aW9uOiB7XG4gICAgICAgICAgICAgICAgICAgIGxhdDogNDkuMDAwNzkzLFxuICAgICAgICAgICAgICAgICAgICBsbmc6IDguNDEwNjg0XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBpbmFjdGl2ZTogdHJ1ZVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgdGl0bGU6ICdIb3RlbCBOYXNzYXVlciBIb2YgJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdLcmllZ3NzdHIuIDg4JyxcbiAgICAgICAgICAgICAgICBsb2NhdGlvbjoge1xuICAgICAgICAgICAgICAgICAgICBsYXQ6IDQ5LjAwNjAyMSxcbiAgICAgICAgICAgICAgICAgICAgbG5nOiA4LjQwNzIwM1xuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgaW5hY3RpdmU6IHRydWVcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnSsO8ZGlzY2hlIFNjaHVsZSBpbiBkZXIgTGlkZWxsc2NodWxlJyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdNYXJrZ3JhZmVuc3RyYcOfZScsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMDc3NjIsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40MDgwMTlcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZTogJ0thdWZoYXVzIEhlcnRpZScsXG4gICAgICAgICAgICAgICAgc3RyZWV0OiAnS2Fpc2Vyc3RyYcOfZSA5MicsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMTAwOTgsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40MDEyMTNcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnS2F1ZmhhdXMgS2Fyc3RhZHQnLFxuICAgICAgICAgICAgICAgIHN0cmVldDogJ0thaXNlcnN0cmHDn2UgMTQ3LTE1OScsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMDk1MjMsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40MDIwMzBcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnTHVkd2lnLU1hcnVtLVN0cmHDn2UnLFxuICAgICAgICAgICAgICAgIHN0cmVldDogJ0x1ZHdpZy1NYXJ1bS1TdHJhw59lJyxcbiAgICAgICAgICAgICAgICBsb2NhdGlvbjoge1xuICAgICAgICAgICAgICAgICAgICBsYXQ6IDQ5LjAxNDMwNCxcbiAgICAgICAgICAgICAgICAgICAgbG5nOiA4LjM2ODI4MlxuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgaW5hY3RpdmU6IHRydWVcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgdGl0bGU6ICdNYXJrdHBsYXR6JyxcbiAgICAgICAgICAgICAgICBzdHJlZXQ6ICdLYXJsLUZyaWVkcmljaC1TdHJhw59lIDcnLFxuICAgICAgICAgICAgICAgIGxvY2F0aW9uOiB7XG4gICAgICAgICAgICAgICAgICAgIGxhdDogNDkuMDA5NDEwLFxuICAgICAgICAgICAgICAgICAgICBsbmc6IDguNDAzOTEyXG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBpbmFjdGl2ZTogdHJ1ZVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZTogJ1BvbGl6ZWlwcsOkc2lkaXVtIE1hcmt0cGxhdHonLFxuICAgICAgICAgICAgICAgIHN0cmVldDogJycsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMTA1NDMsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40MDIyMzhcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnU2NobG9zc3BsYXR6IC0gQsO8Y2hlcnZlcmJyZW5udW5nIGltIEp1bmkgMTkzMycsXG4gICAgICAgICAgICAgICAgc3RyZWV0OiAnU2NobG9zc3BsYXR6JyxcbiAgICAgICAgICAgICAgICBsb2NhdGlvbjoge1xuICAgICAgICAgICAgICAgICAgICBsYXQ6IDQ5LjAxMDk5OSxcbiAgICAgICAgICAgICAgICAgICAgbG5nOiA4LjQwMzg1NFxuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgaW5hY3RpdmU6IHRydWVcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgdGl0bGU6ICdTdGFhdGxpY2hlIEt1bnN0aGFsbGUnLFxuICAgICAgICAgICAgICAgIHN0cmVldDogJ0hhbnMtVGhvbWEtU3RyLiAyLTYnLFxuICAgICAgICAgICAgICAgIGxvY2F0aW9uOiB7XG4gICAgICAgICAgICAgICAgICAgIGxhdDogNDkuMDEyMDA2LFxuICAgICAgICAgICAgICAgICAgICBsbmc6IDguMzk5OTQ5XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBpbmFjdGl2ZTogdHJ1ZVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZTogJ1N5bmFnb2dlIGluIGRlciAtIEthcmwtRnJpZWRyaWNoLVN0cmHDn2UgMTQtMTgnLFxuICAgICAgICAgICAgICAgIHN0cmVldDogJ0thcmwtRnJpZWRyaWNoLVN0cmHDn2UgMTQtMTgnLFxuICAgICAgICAgICAgICAgIGxvY2F0aW9uOiB7XG4gICAgICAgICAgICAgICAgICAgIGxhdDogNDkuMDA3ODc2LFxuICAgICAgICAgICAgICAgICAgICBsbmc6IDguNDAzNTE3XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBpbmFjdGl2ZTogdHJ1ZVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZTogJ1N5bmFnb2dlIEtyb25lbnN0cmHDn2UnLFxuICAgICAgICAgICAgICAgIHN0cmVldDogJ0tyb25lbnN0cmHDn2UgMTUnLFxuICAgICAgICAgICAgICAgIGxvY2F0aW9uOiB7XG4gICAgICAgICAgICAgICAgICAgIGxhdDogNDkuMDA5OTI2LFxuICAgICAgICAgICAgICAgICAgICBsbmc6IDguNDA4Mjc2XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBpbmFjdGl2ZTogdHJ1ZVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgICB0aXRsZTogJ1RlY2huaXNjaGUgSG9jaHNjaHVsZScsXG4gICAgICAgICAgICAgICAgc3RyZWV0OiAnS2Fpc2Vyc3RyYcOfZSAxMicsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMDk2NzUsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40MTE2MTNcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnXCJWb2xrc2hhdXNcIicsXG4gICAgICAgICAgICAgICAgc3RyZWV0OiAnU2Now7x0emVuc3RyYcOfZSAxNCcsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMDI0MzAsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40MDQ5NDJcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnV29obmhhdXMgS3JldXpzdHJhw59lIDYtOCcsXG4gICAgICAgICAgICAgICAgc3RyZWV0OiAnS3JldXpzdHJhw59lIDYtOCcsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMTAxNDYsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC40MDUyMjlcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICAgIHRpdGxlOiAnWmVudHJhbHZlcmJhbmQgZGVyIEFuZ2VzdGVsbHRlbicsXG4gICAgICAgICAgICAgICAgc3RyZWV0OiAnR2FydGVuc3RyYcOfZSAyNScsXG4gICAgICAgICAgICAgICAgbG9jYXRpb246IHtcbiAgICAgICAgICAgICAgICAgICAgbGF0OiA0OS4wMDQ3NDUsXG4gICAgICAgICAgICAgICAgICAgIGxuZzogOC4zOTI5NzBcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIGluYWN0aXZlOiB0cnVlXG4gICAgICAgICAgICB9LFxuXG4gICAgICAgIF0sXG4gICAgICAgIHN0YXJ0UG9pbnQ6ICcnLFxuICAgICAgICBlbmRQb2ludDogJycsXG4gICAgICAgIG1hcmtlcnM6IHtcbiAgICAgICAgICAgIHdheXBvaW50czogW10sXG4gICAgICAgICAgICB3YXlwb2ludHNfaW5hY3RpdmU6IFtdLFxuICAgICAgICAgICAgdXNlckxvY2F0aW9uOiBbXSxcbiAgICAgICAgICAgIHdpbmRvd3M6IFtdXG4gICAgICAgIH0sXG4gICAgICAgIGFsbE1hcmtlcnNCb3VuZDogZmFsc2UsXG4gICAgICAgIG92ZXJsYXk6IGZhbHNlXG4gICAgfSxcbiAgICBpbml0OiBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIG1hcCA9IGRvY3VtZW50LmdldEVsZW1lbnRCeUlkKCdtYXAnKTtcblxuICAgICAgICBpZighbWFwKSB7XG4gICAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cblxuICAgICAgICBOU0lOS0FfTWFwLnZhcnMuc3RhcnRQb2ludCA9IE5TSU5LQV9NYXAudmFycy53YXlwb2ludHNbMF0ubG9jYXRpb247XG4gICAgICAgIE5TSU5LQV9NYXAudmFycy5lbmRQb2ludCA9IE5TSU5LQV9NYXAudmFycy53YXlwb2ludHNbTlNJTktBX01hcC52YXJzLndheXBvaW50cy5sZW5ndGgtMV0ubG9jYXRpb247XG5cbiAgICAgICAgTlNJTktBX01hcC52YXJzLmRpcmVjdGlvbnNTZXJ2aWNlID0gbmV3IGdvb2dsZS5tYXBzLkRpcmVjdGlvbnNTZXJ2aWNlO1xuXG4gICAgICAgIE5TSU5LQV9NYXAudmFycy5hbGxNYXJrZXJzQm91bmQgPSBuZXcgZ29vZ2xlLm1hcHMuTGF0TG5nQm91bmRzKCk7XG5cbiAgICAgICAgTlNJTktBX01hcC52YXJzLm1hcCA9IG5ldyBnb29nbGUubWFwcy5NYXAobWFwLCB7XG4gICAgICAgICAgICBjZW50ZXI6IHtcbiAgICAgICAgICAgICAgICBsYXQ6IDQ5LjAxNTc2NDMsXG4gICAgICAgICAgICAgICAgbG5nOiA4LjI2OTg0NTdcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICBkaXNhYmxlRGVmYXVsdFVJOiB0cnVlLFxuICAgICAgICAgICAgem9vbUNvbnRyb2w6IHRydWUsXG4gICAgICAgICAgICB6b29tQ29udHJvbE9wdGlvbnM6IHtcbiAgICAgICAgICAgICAgICBwb3NpdGlvbjogZ29vZ2xlLm1hcHMuQ29udHJvbFBvc2l0aW9uLkxFRlRfQk9UVE9NXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgem9vbTogMTNcbiAgICAgICAgfSk7XG5cbiAgICAgICAgTlNJTktBX01hcC52YXJzLm92ZXJsYXkgPSBuZXcgZ29vZ2xlLm1hcHMuT3ZlcmxheVZpZXcoKTtcbiAgICAgICAgTlNJTktBX01hcC52YXJzLm92ZXJsYXkuZHJhdyA9IGZ1bmN0aW9uKCkge307XG4gICAgICAgIE5TSU5LQV9NYXAudmFycy5vdmVybGF5LnNldE1hcChOU0lOS0FfTWFwLnZhcnMubWFwKTtcblxuXG4gICAgICAgICQod2luZG93KS5vbignbGF5ZXJDaGFuZ2UnLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgIGdvb2dsZS5tYXBzLmV2ZW50LnRyaWdnZXIoTlNJTktBX01hcC52YXJzLm1hcCwgXCJyZXNpemVcIik7XG4gICAgICAgICAgICAvKnZhciBsaXN0ZW5lciA9IGdvb2dsZS5tYXBzLmV2ZW50LmFkZExpc3RlbmVyKE5TSU5LQV9NYXAudmFycy5tYXAsIFwiaWRsZVwiLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICBOU0lOS0FfTWFwLnZhcnMubWFwLnNldFpvb20oMTMpO1xuICAgICAgICAgICAgICAgIGdvb2dsZS5tYXBzLmV2ZW50LnJlbW92ZUxpc3RlbmVyKGxpc3RlbmVyKTtcbiAgICAgICAgICAgIH0pOyAgKi9cbiAgICAgICAgICAgIE5TSU5LQV9NYXAucmVDZW50ZXIoKTtcbiAgICAgICAgfSk7XG5cbiAgICAgICAgJC5lYWNoKCQuZXh0ZW5kKHRydWUsIHt9LCBOU0lOS0FfTWFwLnZhcnMud2F5cG9pbnRzKSwgZnVuY3Rpb24oaSwgd2F5cG9pbnQpIHtcbiAgICAgICAgICAgIE5TSU5LQV9NYXAudmFycy5hbGxNYXJrZXJzQm91bmQuZXh0ZW5kKG5ldyBnb29nbGUubWFwcy5MYXRMbmcod2F5cG9pbnQubG9jYXRpb24ubGF0LCB3YXlwb2ludC5sb2NhdGlvbi5sbmcpKTtcbiAgICAgICAgfSlcblxuICAgICAgICAvL05TSU5LQV9NYXAudmFycy5pbmZvV2luZG93ID0gbmV3IGdvb2dsZS5tYXBzLkluZm9XaW5kb3coe21hcDogTlNJTktBX01hcC52YXJzLm1hcH0pO1xuICAgICAgICBOU0lOS0FfTWFwLmluaXRSb3V0ZSgpO1xuICAgICAgICBOU0lOS0FfTWFwLnJlQ2VudGVyKCk7XG4gICAgICAgIE5TSU5LQV9NYXAuZ2VuVXNlckxvY2F0aW9uTWFrZXIoKTtcbiAgICAgICAgTlNJTktBX01hcC5nZW5Sb3V0ZSgpO1xuICAgIH0sXG4gICAgaW5pdFJvdXRlOiBmdW5jdGlvbihzZWxlY3QpIHtcbiAgICAgICAgTlNJTktBX01hcC52YXJzLmN1cnJlbnRSb3V0ZSA9IHNlbGVjdCB8fMKgTlNJTktBX01hcC52YXJzLmN1cnJlbnRSb3V0ZTtcblxuICAgICAgICAkLmVhY2goTlNJTktBX01hcC52YXJzLndheXBvaW50cywgZnVuY3Rpb24oaSwgZWxlbSkge1xuICAgICAgICAgICAgaWYoJC5pbkFycmF5KGksIE5TSU5LQV9NYXAudmFycy5yb3V0ZXNbTlNJTktBX01hcC52YXJzLmN1cnJlbnRSb3V0ZV0ucm91dGUpICE9IC0xKSB7XG4gICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLndheXBvaW50c1tpXS5pbmFjdGl2ZSA9IGZhbHNlO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICBOU0lOS0FfTWFwLnZhcnMud2F5cG9pbnRzW2ldLmluYWN0aXZlID0gdHJ1ZTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgfSk7XG5cbiAgICB9LFxuICAgIGluaXRDdXN0b21Sb3V0ZTogZnVuY3Rpb24oKSB7XG4gICAgICAgICQuZWFjaChOU0lOS0FfTWFwLnZhcnMucm91dGVzLCBmdW5jdGlvbihpLCByb3V0ZSkge1xuICAgICAgICAgICAgaWYocm91dGUudHlwZSkge1xuICAgICAgICAgICAgICAgIGlmKHJvdXRlLnR5cGUgPT0gJ2N1c3RvbScpIHtcbiAgICAgICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLmN1cnJlbnRSb3V0ZSA9IGk7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICB9LFxuICAgIHJlQ2VudGVyOiBmdW5jdGlvbigpIHtcbiAgICAgICAgTlNJTktBX01hcC5nZXRVc2VyTG9jYXRpb24oZnVuY3Rpb24ocG9zKSB7XG4gICAgICAgICAgICBpZihwb3MpIHtcbiAgICAgICAgICAgICAgICBOU0lOS0FfTWFwLnZhcnMubWFwLnNldENlbnRlcihuZXcgZ29vZ2xlLm1hcHMuTGF0TG5nKHBvcy5jb29yZHMubGF0aXR1ZGUscG9zLmNvb3Jkcy5sb25naXR1ZGUpKTtcbiAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLm1hcC5maXRCb3VuZHMoTlNJTktBX01hcC52YXJzLmFsbE1hcmtlcnNCb3VuZCk7XG4gICAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgIH0sXG4gICAgZ2VuVXNlckxvY2F0aW9uTWFrZXI6IGZ1bmN0aW9uKCkge1xuXG4gICAgICAgIHZhciByID0gNjtcbiAgICAgICAgTlNJTktBX01hcC52YXJzLnVzZXJHZW9Qb3NpdGlvbi5iYWNrZ3JvdW5kID0gbmV3IGdvb2dsZS5tYXBzLk1hcmtlcih7XG4gICAgICAgICAgICBjbGlja2FibGU6IGZhbHNlLFxuICAgICAgICAgICAgaWNvbjoge1xuICAgICAgICAgICAgICAgIHBhdGg6ICdNLScrKHIpKycsMGEnKyhyKSsnLCcrKHIpKycgMCAxLDAgJysocioyKSsnLDBhJysocikrJywnKyhyKSsnIDAgMSwwIC0nKyhyKjIpKycsMCcsXG4gICAgICAgICAgICAgICAgZmlsbENvbG9yOiAnIzAwNjZDQycsXG4gICAgICAgICAgICAgICAgZmlsbE9wYWNpdHk6IDEsXG4gICAgICAgICAgICAgICAgYW5jaG9yOiBuZXcgZ29vZ2xlLm1hcHMuUG9pbnQoMCwwKSxcbiAgICAgICAgICAgICAgICBzdHJva2VDb2xvcjogJyMwMDY2Q0MnLFxuICAgICAgICAgICAgICAgIHN0cm9rZVdlaWdodDogNDAsXG4gICAgICAgICAgICAgICAgc3Ryb2tlT3BhY2l0eTogLjI1LFxuICAgICAgICAgICAgICAgIHNjYWxlOiAxXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgc2hhZG93OiBudWxsLFxuICAgICAgICAgICAgekluZGV4OiA5OTksXG4gICAgICAgICAgICBtYXA6IE5TSU5LQV9NYXAudmFycy5tYXBcbiAgICAgICAgfSk7XG4gICAgICAgIHZhciBzID0gNDtcbiAgICAgICAgTlNJTktBX01hcC52YXJzLnVzZXJHZW9Qb3NpdGlvbi5hcnJvdyA9IG5ldyBnb29nbGUubWFwcy5NYXJrZXIoe1xuICAgICAgICAgICAgY2xpY2thYmxlOiBmYWxzZSxcbiAgICAgICAgICAgIGljb246IHtcbiAgICAgICAgICAgICAgICBwYXRoOiAnTSAwIDAgTCAnKyhzKjIpKycgMCBMICcrKHMpKycgJysocyoyKSsnIHonLFxuICAgICAgICAgICAgICAgIGZpbGxDb2xvcjogJyMwMDY2Q0MnLFxuICAgICAgICAgICAgICAgIGZpbGxPcGFjaXR5OiAwLFxuICAgICAgICAgICAgICAgIGFuY2hvcjogbmV3IGdvb2dsZS5tYXBzLlBvaW50KChzLzIpLCgocy8yKS0ocis1KSkpLFxuICAgICAgICAgICAgICAgIHN0cm9rZVdlaWdodDogMCxcbiAgICAgICAgICAgICAgICBzY2FsZTogMSxcbiAgICAgICAgICAgICAgICByb3RhdGlvbjogNDVcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICBzaGFkb3c6IG51bGwsXG4gICAgICAgICAgICB6SW5kZXg6IDk5OSxcbiAgICAgICAgICAgIG1hcDogTlNJTktBX01hcC52YXJzLm1hcFxuICAgICAgICB9KTtcblxuXG4gICAgICAgIE5TSU5LQV9NYXAudmFycy51c2VyR2VvUG9zaXRpb24uc2V0UG9zaXRpb24gPSBmdW5jdGlvbihwb3MsIGNvbXBhc3MpIHtcblxuICAgICAgICAgICAgaWYocG9zKSB7XG4gICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLnVzZXJHZW9Qb3NpdGlvbi5iYWNrZ3JvdW5kLnNldFBvc2l0aW9uKHBvcyk7XG4gICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLnVzZXJHZW9Qb3NpdGlvbi5hcnJvdy5zZXRQb3NpdGlvbihwb3MpO1xuICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICBpZihjb21wYXNzKSB7XG4gICAgICAgICAgICAgICAgdmFyIGljb24gPSBOU0lOS0FfTWFwLnZhcnMudXNlckdlb1Bvc2l0aW9uLmFycm93LmdldEljb24oKVxuICAgICAgICAgICAgICAgIGljb24ucm90YXRpb24gPSBjb21wYXNzLTE4MDtcbiAgICAgICAgICAgICAgICBpY29uLmZpbGxPcGFjaXR5ID0gMTtcbiAgICAgICAgICAgICAgICBOU0lOS0FfTWFwLnZhcnMudXNlckdlb1Bvc2l0aW9uLmFycm93LnNldEljb24oaWNvbik7XG4gICAgICAgICAgICB9XG5cbiAgICAgICAgfVxuXG4gICAgICAgIC8vIG9uIGNvbXBhc3MgY2hhbmdlXG4gICAgICAgIGlmICh3aW5kb3cuRGV2aWNlT3JpZW50YXRpb25FdmVudCkge1xuICAgICAgICAgICAgd2luZG93LmFkZEV2ZW50TGlzdGVuZXIoJ2RldmljZW9yaWVudGF0aW9uJywgZnVuY3Rpb24oZXZlbnREYXRhKSB7XG4gICAgICAgICAgICAgICAgdmFyIGNvbXBhc3NkaXI7XG4gICAgICAgICAgICAgICAgaWYoZXZlbnQud2Via2l0Q29tcGFzc0hlYWRpbmcpIHtcbiAgICAgICAgICAgICAgICAgICAgY29tcGFzc2RpciA9IGV2ZW50LndlYmtpdENvbXBhc3NIZWFkaW5nO1xuICAgICAgICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICAgICAgICAgIGNvbXBhc3NkaXIgPSBldmVudC5hbHBoYTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLnVzZXJHZW9Qb3NpdGlvbi5zZXRQb3NpdGlvbihudWxsLCBjb21wYXNzZGlyKTtcbiAgICAgICAgICAgICAgICAkKHdpbmRvdykudHJpZ2dlcignY29tcGFzc0hlYWRpbmcnLCBjb21wYXNzZGlyKTtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgbmF2aWdhdG9yLmdlb2xvY2F0aW9uLmdldEN1cnJlbnRQb3NpdGlvbih0aGlzLnNldFVzZXJMb2NhdGlvbiwgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICBjb25zb2xlLmVycm9yKCdubyBsb2NhdGlvbicpO1xuICAgICAgICB9KTtcblxuICAgICAgICAvLyBvbiBwb3NpdGlvbiBjaGFuZ2VcbiAgICAgICAgbmF2aWdhdG9yLmdlb2xvY2F0aW9uLndhdGNoUG9zaXRpb24odGhpcy5zZXRVc2VyTG9jYXRpb24pO1xuXG5cbiAgICB9LFxuICAgIGdldFVzZXJMb2NhdGlvbjogZnVuY3Rpb24oZm5BZnRlcikge1xuICAgICAgICBuYXZpZ2F0b3IuZ2VvbG9jYXRpb24uZ2V0Q3VycmVudFBvc2l0aW9uKGZuQWZ0ZXIsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgZm5BZnRlcihmYWxzZSk7XG4gICAgICAgIH0pO1xuICAgIH0sXG5cbiAgICBzZXRVc2VyTG9jYXRpb246IGZ1bmN0aW9uKHBvcykge1xuICAgICAgICBOU0lOS0FfTWFwLnZhcnMudXNlckdlb1Bvc2l0aW9uLmxhdExuZyA9IG5ldyBnb29nbGUubWFwcy5MYXRMbmcocG9zLmNvb3Jkcy5sYXRpdHVkZSwgcG9zLmNvb3Jkcy5sb25naXR1ZGUpO1xuICAgICAgICBOU0lOS0FfTWFwLnZhcnMudXNlckdlb1Bvc2l0aW9uLnNldFBvc2l0aW9uKE5TSU5LQV9NYXAudmFycy51c2VyR2VvUG9zaXRpb24ubGF0TG5nKTtcbiAgICB9LFxuICAgIGdlblJvdXRlOiBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHpvb20gPSBOU0lOS0FfTWFwLnZhcnMubWFwLmdldFpvb20oKTtcbiAgICAgICAgaWYoTlNJTktBX01hcC52YXJzLmRpcmVjdGlvbnNEaXNwbGF5KXtcbiAgICAgICAgICAgIC8vTlNJTktBX01hcC52YXJzLmRpcmVjdGlvbnNTZXJ2aWNlLnNldE1hcChudWxsKTtcbiAgICAgICAgICAgIE5TSU5LQV9NYXAudmFycy5kaXJlY3Rpb25zRGlzcGxheS5zZXRNYXAobnVsbCk7XG4gICAgICAgIH1cblxuICAgICAgICBOU0lOS0FfTWFwLnZhcnMuZGlyZWN0aW9uc0Rpc3BsYXkgPSBuZXcgZ29vZ2xlLm1hcHMuRGlyZWN0aW9uc1JlbmRlcmVyKHtcbiAgICAgICAgICAgIHBvbHlsaW5lT3B0aW9uczoge1xuICAgICAgICAgICAgICAgIHN0cm9rZUNvbG9yOiBcIiMwMDAwMDBcIlxuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHN1cHByZXNzTWFya2VyczogdHJ1ZVxuICAgICAgICB9KTtcblxuICAgICAgICBOU0lOS0FfTWFwLnZhcnMuZGlyZWN0aW9uc0Rpc3BsYXkuc2V0TWFwKE5TSU5LQV9NYXAudmFycy5tYXApO1xuXG4gICAgICAgIHZhciB3YXlwb2ludHNfYWN0aXZlID0gW10sXG4gICAgICAgICAgICB3YXlwb2ludHNfaW5hY3RpdmUgPSBbXTtcblxuICAgICAgICAkLmVhY2goJC5leHRlbmQodHJ1ZSwge30sIE5TSU5LQV9NYXAudmFycy53YXlwb2ludHMpLCBmdW5jdGlvbihpLCB3YXlwb2ludCkge1xuICAgICAgICAgICAgaWYodHlwZW9mIHdheXBvaW50LmluYWN0aXZlICE9ICd1bmRlZmluZWQnKSB7XG4gICAgICAgICAgICAgICAgaWYod2F5cG9pbnQuaW5hY3RpdmUpIHtcbiAgICAgICAgICAgICAgICAgICAgd2F5cG9pbnRzX2luYWN0aXZlLnB1c2god2F5cG9pbnQpO1xuICAgICAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICAgICAgd2F5cG9pbnRzX2FjdGl2ZS5wdXNoKHdheXBvaW50KTtcbiAgICAgICAgfSlcblxuICAgICAgICB2YXIgYWN0aXZlX2xlbmd0aCA9IE9iamVjdC5rZXlzKHdheXBvaW50c19hY3RpdmUpLmxlbmd0aDtcblxuXG4gICAgICAgIGlmKGFjdGl2ZV9sZW5ndGgpIHtcblxuICAgICAgICAgICAgICAgIHZhciBzdGFydCA9IE5TSU5LQV9NYXAudmFycy51c2VyR2VvUG9zaXRpb24ubGF0TG5nID9cbiAgICAgICAgICAgICAgICAgICAgICAgIE5TSU5LQV9NYXAudmFycy51c2VyR2VvUG9zaXRpb24ubGF0TG5nIDpcbiAgICAgICAgICAgICAgICAgICAgICAgIG5ldyBnb29nbGUubWFwcy5MYXRMbmcod2F5cG9pbnRzX2FjdGl2ZVswXS5sb2NhdGlvbiksXG4gICAgICAgICAgICAgICAgICAgIGVuZCA9XG4gICAgICAgICAgICAgICAgICAgICAgICBuZXcgZ29vZ2xlLm1hcHMuTGF0TG5nKHdheXBvaW50c19hY3RpdmVbYWN0aXZlX2xlbmd0aC0xXS5sb2NhdGlvbilcbiAgICAgICAgICAgICAgICAgICAgO1xuXG5cbiAgICAgICAgICAgICAgICB0aGlzLnZhcnMuZGlyZWN0aW9uc1NlcnZpY2Uucm91dGUoe1xuICAgICAgICAgICAgICAgICAgICBvcmlnaW46IHN0YXJ0LFxuICAgICAgICAgICAgICAgICAgICBkZXN0aW5hdGlvbjogZW5kLFxuICAgICAgICAgICAgICAgICAgICB3YXlwb2ludHM6ICQubWFwKCQuZXh0ZW5kKHRydWUsIHt9LCB3YXlwb2ludHNfYWN0aXZlKSwgZnVuY3Rpb24od2F5cG9pbnQpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybiB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgbG9jYXRpb24gOiB3YXlwb2ludC5sb2NhdGlvblxuICAgICAgICAgICAgICAgICAgICAgICAgfTtcbiAgICAgICAgICAgICAgICAgICAgfSksXG4gICAgICAgICAgICAgICAgICAgIG9wdGltaXplV2F5cG9pbnRzOiB0cnVlLFxuICAgICAgICAgICAgICAgICAgICB0cmF2ZWxNb2RlOiBnb29nbGUubWFwcy5UcmF2ZWxNb2RlLldBTEtJTkdcbiAgICAgICAgICAgICAgICB9LCBmdW5jdGlvbihyZXNwb25zZSwgc3RhdHVzKSB7XG4gICAgICAgICAgICAgICAgICAgIGlmIChzdGF0dXMgPT09IGdvb2dsZS5tYXBzLkRpcmVjdGlvbnNTdGF0dXMuT0spIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIE5TSU5LQV9NYXAudmFycy5kaXJlY3Rpb25zRGlzcGxheS5zZXREaXJlY3Rpb25zKHJlc3BvbnNlKTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgcmVzcG9uc2UuZGVzY3JpYmVkX3dheXBvaW50cyA9ICQubWFwKHJlc3BvbnNlLmdlb2NvZGVkX3dheXBvaW50cywgZnVuY3Rpb24od2F5cG9pbnQsIGkpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm4gT2JqZWN0LmFzc2lnbih3YXlwb2ludCwgd2F5cG9pbnRzX2FjdGl2ZVtpXSk7XG4gICAgICAgICAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgcmVzcG9uc2Uuc3lzdGVtX3dheXBvaW50cyA9IE5TSU5LQV9NYXAudmFycy53YXlwb2ludHM7XG5cbiAgICAgICAgICAgICAgICAgICAgICAgICQod2luZG93KS50cmlnZ2VyKCdyb3V0ZUhhc0JlZW5DYWxjdWxhdGVkJywgcmVzcG9uc2UpO1xuXG4gICAgICAgICAgICAgICAgICAgICAgICAkLmVhY2goTlNJTktBX01hcC52YXJzLm1hcmtlcnMud2F5cG9pbnRzLCBmdW5jdGlvbihpLCBpdGVtKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgaXRlbS5zZXRNYXAobnVsbCk7XG4gICAgICAgICAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgdmFyIGxlZyA9IHJlc3BvbnNlLnJvdXRlc1sgMCBdLmxlZ3NbIDAgXTtcbiAgICAgICAgICAgICAgICAgICAgICAgIHZhciByID0gNTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgLyogQWN0aXZlIG1hcmtlcnMgKi9cbiAgICAgICAgICAgICAgICAgICAgICAgIHZhciBsZWdzID0gcmVzcG9uc2Uucm91dGVzWyAwIF0ubGVncztcbiAgICAgICAgICAgICAgICAgICAgICAgICQuZWFjaCh3YXlwb2ludHNfYWN0aXZlLCBmdW5jdGlvbihpLCBtYXJrZXIpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAvKmlmKGkgPT0gKGxlZ3MubGVuZ3RoLTEpIHx8ICFpKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9Ki9cblxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHZhciBfbWFya2VyID0gbmV3IGdvb2dsZS5tYXBzLk1hcmtlcih7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHBvc2l0aW9uOiBuZXcgZ29vZ2xlLm1hcHMuTGF0TG5nKG1hcmtlci5sb2NhdGlvbi5sYXQsIG1hcmtlci5sb2NhdGlvbi5sbmcpLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBtYXA6IE5TSU5LQV9NYXAudmFycy5tYXAsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGljb246IHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHBhdGg6ICdNLScrKHIpKycsMGEnKyhyKSsnLCcrKHIpKycgMCAxLDAgJysocioyKSsnLDBhJysocikrJywnKyhyKSsnIDAgMSwwIC0nKyhyKjIpKycsMCcsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBmaWxsQ29sb3I6ICcjZjZhZTk1JyxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGZpbGxPcGFjaXR5OiAxLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYW5jaG9yOiBuZXcgZ29vZ2xlLm1hcHMuUG9pbnQoMCwwKSxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHN0cm9rZVdlaWdodDogMixcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHNjYWxlOiAyLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG5cbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBfbWFya2VyLm5zaW5rYURhdGEgPSB3YXlwb2ludHNfYWN0aXZlW2ldO1xuXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLm1hcmtlcnMud2F5cG9pbnRzLnB1c2goX21hcmtlcik7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgTlNJTktBX01hcC5tYXJrZXJXaW5kb3coX21hcmtlcik7XG5cbiAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgICAgICAgICAgICAgICBzZXRUaW1lb3V0KGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIE5TSU5LQV9NYXAudmFycy5tYXAuc2V0Wm9vbSgxMSk7XG4gICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHdpbmRvdy5hbGVydCgnRGlyZWN0aW9ucyByZXF1ZXN0IGZhaWxlZCBkdWUgdG8gJyArIHN0YXR1cyk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICB9XG4gICAgICAgIC8qIEluYWN0aXZlIG1hcmtlcnMgKi9cbiAgICAgICAgJC5lYWNoKCQuZXh0ZW5kKHRydWUsIHt9LCB3YXlwb2ludHNfaW5hY3RpdmUpLCBmdW5jdGlvbihpLCBtYXJrZXIpIHtcbiAgICAgICAgICAgIHZhciByID0gNTtcblxuICAgICAgICAgICAgdmFyIF9tYXJrZXIgPSBuZXcgZ29vZ2xlLm1hcHMuTWFya2VyKHtcbiAgICAgICAgICAgICAgICBwb3NpdGlvbjogbmV3IGdvb2dsZS5tYXBzLkxhdExuZyhtYXJrZXIubG9jYXRpb24pLFxuICAgICAgICAgICAgICAgIGljb246IHtcbiAgICAgICAgICAgICAgICAgICAgcGF0aDogJ00tJysocikrJywwYScrKHIpKycsJysocikrJyAwIDEsMCAnKyhyKjIpKycsMGEnKyhyKSsnLCcrKHIpKycgMCAxLDAgLScrKHIqMikrJywwJyxcbiAgICAgICAgICAgICAgICAgICAgZmlsbENvbG9yOiAnIzAwMDAwMCcsXG4gICAgICAgICAgICAgICAgICAgIGZpbGxPcGFjaXR5OiAuNSxcbiAgICAgICAgICAgICAgICAgICAgYW5jaG9yOiBuZXcgZ29vZ2xlLm1hcHMuUG9pbnQoMCwwKSxcbiAgICAgICAgICAgICAgICAgICAgc3Ryb2tlV2VpZ2h0OiAwLFxuICAgICAgICAgICAgICAgICAgICBzY2FsZTogMSxcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgX21hcmtlci5uc2lua2FEYXRhID0gbWFya2VyO1xuXG4gICAgICAgICAgICBOU0lOS0FfTWFwLm1hcmtlcldpbmRvdyhfbWFya2VyKTtcblxuICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLm1hcmtlcnMud2F5cG9pbnRzX2luYWN0aXZlLnB1c2goX21hcmtlcik7XG5cbiAgICAgICAgICAgIF9tYXJrZXIuc2V0TWFwKE5TSU5LQV9NYXAudmFycy5tYXApO1xuXG4gICAgICAgIH0pO1xuXG4gICAgICAgIGlmKCFPYmplY3Qua2V5cyh3YXlwb2ludHNfYWN0aXZlKS5sZW5ndGgpIHtcbiAgICAgICAgICAgIHNldFRpbWVvdXQoZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgJCh3aW5kb3cpLnRyaWdnZXIoJ3JvdXRlSGFzQmVlbkNhbGN1bGF0ZWQnLCB7fSk7XG4gICAgICAgICAgICB9LCAxMDAwKVxuICAgICAgICB9XG4gICAgfSxcbiAgICBoYW5kbGVFcnJvcjogZnVuY3Rpb24oYnJvd3Nlckhhc0dlb2xvY2F0aW9uLCBwb3MpIHtcbiAgICAgICAgTlNJTktBX01hcC52YXJzLmluZm9XaW5kb3cuc2V0UG9zaXRpb24ocG9zKTtcbiAgICAgICAgTlNJTktBX01hcC52YXJzLmluZm9XaW5kb3cuc2V0Q29udGVudChicm93c2VySGFzR2VvbG9jYXRpb24gP1xuICAgICAgICAnRXJyb3I6IFRoZSBHZW9sb2NhdGlvbiBzZXJ2aWNlIGZhaWxlZC4nIDpcbiAgICAgICAgJ0Vycm9yOiBZb3VyIGJyb3dzZXIgZG9lc25cXCd0IHN1cHBvcnQgZ2VvbG9jYXRpb24uJyk7XG4gICAgfSxcbiAgICBtYXJrZXJXaW5kb3c6IGZ1bmN0aW9uKG1hcmtlcikge1xuICAgICAgICBtYXJrZXIuYWRkTGlzdGVuZXIoJ2NsaWNrJywgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICB2YXIgJHdpbiA9ICQoTlNJTktBX0dsb2JhbC50bXBsKCdtYXBXaW5kb3cnLCBtYXJrZXIubnNpbmthRGF0YSkpO1xuXG4gICAgICAgICAgICBOU0lOS0FfTWFwLnZhcnMubWFwLnBhblRvKG1hcmtlci5nZXRQb3NpdGlvbigpKTtcbiAgICAgICAgICAgIGdvb2dsZS5tYXBzLmV2ZW50LmFkZExpc3RlbmVyT25jZShOU0lOS0FfTWFwLnZhcnMubWFwLCAnaWRsZScsIGZ1bmN0aW9uKCl7XG4gICAgICAgICAgICAgICAgJCgnI3BsYWNlaG9sZGVyJykuYXBwZW5kKCR3aW4pO1xuICAgICAgICAgICAgICAgIHNldFBvcygkd2luKTtcblxuICAgICAgICAgICAgICAgICR3aW5bMF0uZXZlbnRfZHJhZyA9IE5TSU5LQV9NYXAudmFycy5tYXAuYWRkTGlzdGVuZXIoJ2JvdW5kc19jaGFuZ2VkJywgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgICAgICR3aW4udHJpZ2dlcignY2xvc2UnKVxuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICR3aW5bMF0uZXZlbnRfem9vbSA9IE5TSU5LQV9NYXAudmFycy5tYXAuYWRkTGlzdGVuZXIoJ3pvb21fY2hhbmdlZCcsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICAgICAkd2luLnRyaWdnZXIoJ2Nsb3NlJylcbiAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgICAgICQuZWFjaChOU0lOS0FfTWFwLnZhcnMubWFya2Vycy53aW5kb3dzLCBmdW5jdGlvbihpLCB3aW5kb3cpIHtcbiAgICAgICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLm1hcmtlcnMud2luZG93cy50cmlnZ2VyKCdjbG9zZScpO1xuICAgICAgICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgICAgICAgTlNJTktBX01hcC52YXJzLm1hcmtlcnMud2luZG93cyA9ICR3aW47XG5cbiAgICAgICAgICAgICAgICAkd2luLmZpbmQoJy5jbG9zZScpLmNsaWNrKGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICAgICAkd2luLnJlbW92ZSgpO1xuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgIC8vaW5mb3dpbmRvdy5vcGVuKE5TSU5LQV9NYXAudmFycy5tYXAsIG1hcmtlcik7XG5cblxuICAgICAgICAgICAgICAgICR3aW4ub24oJ2Nsb3NlJywgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgICAgICQodGhpcykucmVtb3ZlKCk7XG4gICAgICAgICAgICAgICAgICAgIGdvb2dsZS5tYXBzLmV2ZW50LnJlbW92ZUxpc3RlbmVyKHRoaXMuZXZlbnRfZHJhZyk7XG4gICAgICAgICAgICAgICAgICAgIGdvb2dsZS5tYXBzLmV2ZW50LnJlbW92ZUxpc3RlbmVyKHRoaXMuZXZlbnRfem9vbSk7XG4gICAgICAgICAgICAgICAgfSk7XG5cbiAgICAgICAgICAgIH0pO1xuXG5cblxuXG5cbiAgICAgICAgfSk7XG5cblxuICAgICAgICBmdW5jdGlvbiBzZXRQb3MoJGVsZW0pIHtcbiAgICAgICAgICAgIHZhciBnbWFwID0gTlNJTktBX01hcC52YXJzLm1hcDtcbiAgICAgICAgICAgIHZhciB0b3BSaWdodD1nbWFwLmdldFByb2plY3Rpb24oKS5mcm9tTGF0TG5nVG9Qb2ludChnbWFwLmdldEJvdW5kcygpLmdldE5vcnRoRWFzdCAoKSk7XG4gICAgICAgICAgICB2YXIgYm90dG9tTGVmdD1nbWFwLmdldFByb2plY3Rpb24oKS5mcm9tTGF0TG5nVG9Qb2ludChnbWFwLmdldEJvdW5kcygpLmdldFNvdXRoV2VzdCgpKTtcbiAgICAgICAgICAgIHZhciBzY2FsZT1NYXRoLnBvdygyLGdtYXAuZ2V0Wm9vbSgpKTtcbiAgICAgICAgICAgIHZhciB3b3JsZFBvaW50PWdtYXAuZ2V0UHJvamVjdGlvbigpLmZyb21MYXRMbmdUb1BvaW50KG1hcmtlci5nZXRQb3NpdGlvbigpKTtcbiAgICAgICAgICAgIHZhciBwb2ludCA9IG5ldyBnb29nbGUubWFwcy5Qb2ludCgod29ybGRQb2ludC54LWJvdHRvbUxlZnQueCkqc2NhbGUsKHdvcmxkUG9pbnQueS10b3BSaWdodC55KSpzY2FsZSk7XG5cbiAgICAgICAgICAgICRlbGVtLmNzcyh7XG4gICAgICAgICAgICAgICAgbGVmdDooIHBvaW50LnggLSAkZWxlbS53aWR0aCgpLzIpLFxuICAgICAgICAgICAgICAgIHRvcDoocG9pbnQueSAtICRlbGVtLmhlaWdodCgpIC0gMzApXG4gICAgICAgICAgICB9KTtcblxuICAgICAgICB9XG4gICAgfSxcbiAgICBzdGFydERpcmVjdGlvbnM6IGZ1bmN0aW9uKCkge1xuXG5cblxuICAgIH1cbn1cblxuZnVuY3Rpb24gTlNJTktBX2dNYXAoKSB7XG4gICAgTlNJTktBX01hcC5pbml0KCk7XG59XG4iLCIvKipcbiogRGVzY3JpcHRpb24gZm9yIE5TSU5LQV9MYXllcnNcbiogQHByaXZhdGVcbiogQHByb3BlcnR5IE5TSU5LQV9MYXllcnNcbiovXG52YXIgTlNJTktBX1NwZWNpZmljcyA9IHtcbiAgICB2YXJzOiB7XG4gICAgfSxcbiAgICBpbml0OiBmdW5jdGlvbigpIHtcbiAgICAgICAgTlNJTktBX1NwZWNpZmljcy5zcGVjaWZpY3MuZGVza3RvcC5tYXBMYXlvdXQgPSBOU0lOS0FfU3BlY2lmaWNzLnNwZWNpZmljcy50YWJsZXQubWFwTGF5b3V0O1xuXG4gICAgICAgICQod2luZG93KS5vbignc21hcnRwaG9uZS1hY3Rpb25zIHRhYmxldC1hY3Rpb25zIGRlc2t0b3AtYWN0aW9ucycsIHRoaXMuYWN0aW9ucy5hdHRhY2gpO1xuICAgIH0sXG4gICAgYWN0aW9uczoge1xuICAgICAgICBhdHRhY2g6IGZ1bmN0aW9uKGUpIHtcblxuICAgICAgICAgICAgdmFyIGFjdGlvbnMgPSBPYmplY3QuYXNzaWduKHt9LCBOU0lOS0FfU3BlY2lmaWNzLnNwZWNpZmljcy5zbWFydHBob25lKSxcbiAgICAgICAgICAgICAgICBhY3Rpb25zID0gT2JqZWN0LmFzc2lnbihhY3Rpb25zLCBOU0lOS0FfU3BlY2lmaWNzLnNwZWNpZmljcy50YWJsZXQpLFxuICAgICAgICAgICAgICAgIGFjdGlvbnMgPSBPYmplY3QuYXNzaWduKGFjdGlvbnMsIE5TSU5LQV9TcGVjaWZpY3Muc3BlY2lmaWNzLmRlc2t0b3ApO1xuXG4gICAgICAgICAgICAkLmVhY2goYWN0aW9ucywgZnVuY3Rpb24oaSwgYWN0aW9uKSB7XG4gICAgICAgICAgICAgICAgTlNJTktBX1NwZWNpZmljcy5hY3Rpb25zLnJlbW92ZShhY3Rpb24pO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAkLmVhY2goTlNJTktBX1NwZWNpZmljcy5zcGVjaWZpY3NbZS50eXBlLnJlcGxhY2UoLy1hY3Rpb25zJC8sJycpXSwgZnVuY3Rpb24oaSwgYWN0aW9uKSB7XG4gICAgICAgICAgICAgICAgJC5lYWNoKGFjdGlvbi5hY3Rpb24uc3BsaXQoJywnKSwgZnVuY3Rpb24oaSwgX2FjdGlvbikge1xuICAgICAgICAgICAgICAgICAgICBhY3Rpb24uJGVsZW0ub24oX2FjdGlvbiwgYWN0aW9uLmZuKTtcbiAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICBhY3Rpb24uJGVsZW0udHJpZ2dlcignX25vdycpO1xuICAgICAgICAgICAgfSk7XG4gICAgICAgIH0sXG4gICAgICAgIHJlbW92ZTogZnVuY3Rpb24oYWN0aW9uKSB7XG4gICAgICAgICAgICAkLmVhY2goYWN0aW9uLmFjdGlvbi5zcGxpdCgnLCcpLCBmdW5jdGlvbihpLCBfYWN0aW9uKSB7XG4gICAgICAgICAgICAgICAgYWN0aW9uLiRlbGVtLm9mZihhY3Rpb24uYWN0aW9uLCBhY3Rpb24uZm4pO1xuICAgICAgICAgICAgICAgIGlmKHR5cGVvZiBhY3Rpb24uZm5PblJlbW92ZSA9PSAnZnVuY3Rpb24nKSB7XG4gICAgICAgICAgICAgICAgICAgIGFjdGlvbi5mbk9uUmVtb3ZlKCk7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfSk7XG5cbiAgICAgICAgfVxuICAgIH0sXG4gICAgc3BlY2lmaWNzOiB7XG4gICAgICAgIHNtYXJ0cGhvbmU6IHtcblxuICAgICAgICB9LFxuICAgICAgICB0YWJsZXQ6IHtcbiAgICAgICAgICAgIG1hcExheW91dDoge1xuICAgICAgICAgICAgICAgIC8qXG4gICAgICAgICAgICAgICAgICogU2V0IG1hcCBsYXlvdXRcbiAgICAgICAgICAgICAgICAgKi9cbiAgICAgICAgICAgICAgICAkZWxlbSA6ICQod2luZG93KSxcbiAgICAgICAgICAgICAgICBhY3Rpb246ICdyZXNpemUsX25vdycsXG4gICAgICAgICAgICAgICAgZm4gOiBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAgICAgdmFyICRxdWlja2xpbmtzID0gJCgnI3F1aWNrbGlua3MnKSxcbiAgICAgICAgICAgICAgICAgICAgICAgICRwYXJlbnQgPSAkcXVpY2tsaW5rcy5wYXJlbnQoKSxcbiAgICAgICAgICAgICAgICAgICAgICAgICRtYXAgPSAkKCcjbWFwJyksXG4gICAgICAgICAgICAgICAgICAgICAgICAkZm9vdGVyID0gJCgnI2Zvb3RlcicpO1xuXG5cbiAgICAgICAgICAgICAgICAgICAgbmV3IFJlc2l6ZVNlbnNvcigkcXVpY2tsaW5rc1swXSwgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAkbWFwLnRyaWdnZXIoJ19yZXNpemUnKTtcbiAgICAgICAgICAgICAgICAgICAgfSk7XG5cbiAgICAgICAgICAgICAgICAgICAgJG1hcC5vbignX3Jlc2l6ZScsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgJG1hcC5jc3Moe1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGhlaWdodDogJCh3aW5kb3cpLmhlaWdodCgpLSRwYXJlbnQub2Zmc2V0KCkudG9wLSRxdWlja2xpbmtzLmhlaWdodCgpXG4gICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICRmb290ZXIuY3NzKHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBoZWlnaHQ6ICQod2luZG93KS5oZWlnaHQoKS0kcGFyZW50Lm9mZnNldCgpLnRvcC0kcXVpY2tsaW5rcy5oZWlnaHQoKSxcbiAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgICAgICAgICB9KS50cmlnZ2VyKCdfcmVzaXplJyk7XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBmbk9uUmVtb3ZlOiBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAgICAgdmFyICRxdWlja2xpbmtzID0gJCgnI3F1aWNrbGlua3MnKSxcbiAgICAgICAgICAgICAgICAgICAgICAgICRtYXAgPSAkKCcjbWFwJyksXG4gICAgICAgICAgICAgICAgICAgICAgICAkZm9vdGVyID0gJCgnI2Zvb3RlcicpO1xuXG4gICAgICAgICAgICAgICAgICAgIG5ldyBSZXNpemVTZW5zb3IuZGV0YWNoKCRxdWlja2xpbmtzWzBdKTtcbiAgICAgICAgICAgICAgICAgICAgJG1hcC5jc3Moe1xuICAgICAgICAgICAgICAgICAgICAgICAgaGVpZ2h0OiAnJ1xuICAgICAgICAgICAgICAgICAgICB9KS5vZmYoJ19yZXNpemUnKTtcbiAgICAgICAgICAgICAgICAgICAgJGZvb3Rlci5jc3Moe1xuICAgICAgICAgICAgICAgICAgICAgICAgaGVpZ2h0OiAnJyxcbiAgICAgICAgICAgICAgICAgICAgfSkub2ZmKCdfcmVzaXplJyk7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICB9LFxuICAgICAgICBkZXNrdG9wOiB7XG4gICAgICAgIH0sXG4gICAgfVxufVxuIiwidmFyIE5TSU5LQV9DaHJvbmljbGUgPSB7XG4gICAgdmFyczoge1xuICAgIH0sXG4gICAgaW5pdDogZnVuY3Rpb24oKSB7XG4gICAgICAgIE5TSU5LQV9DaHJvbmljbGUudmVyc2lvbnMudGFibGV0LmlzSW5pdGVkID0gZmFsc2U7XG4gICAgICAgICQod2luZG93KS5vbigncmVzaXplX19jaHJvbmljbGUnLCBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgIGlmKE5TSU5LQV9HbG9iYWwuaXNfc21hcnRwaG9uZSgpKSB7XG4gICAgICAgICAgICAgICAgTlNJTktBX0Nocm9uaWNsZS52ZXJzaW9ucy5tb2JpbGUuaW5pdCgpO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICBOU0lOS0FfQ2hyb25pY2xlLnZlcnNpb25zLnRhYmxldC5pbml0KCk7XG4gICAgICAgICAgICB9XG4gICAgICAgIH0pLnRyaWdnZXIoJ3Jlc2l6ZV9fY2hyb25pY2xlJyk7XG4gICAgfSxcbiAgICB2ZXJzaW9uczoge1xuICAgICAgICBtb2JpbGU6IHtcbiAgICAgICAgICAgIGluaXQ6IGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgIHRoaXMubGF5ZXJzLmluaXQoKTtcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICBsYXllcnM6IHtcbiAgICAgICAgICAgICAgICBpbml0OiBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAgICAgdmFyIF90aGlzID0gdGhpcyxcbiAgICAgICAgICAgICAgICAgICAgICAgICRoYW5kbGUgPSAkKCcuY2hyb25pY2xlLS1nbG9iYWw+LmNocm9uaWNsZV9faGFuZGxlJykuZmlyc3QoKTtcblxuICAgICAgICAgICAgICAgICAgICAkaGFuZGxlWzBdLiRsYXllciA9ICRoYW5kbGUuY2xvc2VzdCgnYXJ0aWNsZScpO1xuICAgICAgICAgICAgICAgICAgICAkaGFuZGxlWzBdLiRsYXllckNvbnRlbnQgPSAkaGFuZGxlWzBdLiRsYXllci5maW5kKCcuYXJ0aWNsZS1jb250ZW50LXdyYXBwZXInKTtcblxuICAgICAgICAgICAgICAgICAgICAkaGFuZGxlWzBdLm1pbldpZHRoID0gMDtcbiAgICAgICAgICAgICAgICAgICAgJGhhbmRsZVswXS5tYXhXaWR0aCA9ICRoYW5kbGVbMF0uJGxheWVyWzBdLm9mZnNldFdpZHRoLSRoYW5kbGVbMF0ub2Zmc2V0V2lkdGg7XG4gICAgICAgICAgICAgICAgICAgICRoYW5kbGVbMF0ub3BlbmVkID0gZmFsc2U7XG4gICAgICAgICAgICAgICAgICAgICRoYW5kbGVbMF0udG91Y2hzdGFydFdpZHRoID0gJGhhbmRsZVswXS5tYXhXaWR0aDtcblxuICAgICAgICAgICAgICAgICAgICAkaGFuZGxlWzBdLmFkZEV2ZW50TGlzdGVuZXIoJ3RvdWNoc3RhcnQnLCB0aGlzLmV2ZW50SGFuZGxlci50b3VjaHN0YXJ0LCB7IHBhc3NpdmUgOiB0cnVlIH0pO1xuICAgICAgICAgICAgICAgICAgICAkaGFuZGxlWzBdLmFkZEV2ZW50TGlzdGVuZXIoJ3RvdWNobW92ZScsIHRoaXMuZXZlbnRIYW5kbGVyLnRvdWNobW92ZSwgeyBwYXNzaXZlIDogdHJ1ZSB9KTtcbiAgICAgICAgICAgICAgICAgICAgJGhhbmRsZVswXS5hZGRFdmVudExpc3RlbmVyKCd0b3VjaGVuZCcsIHRoaXMuZXZlbnRIYW5kbGVyLnRvdWNoZW5kLCB7IHBhc3NpdmUgOiB0cnVlIH0pO1xuICAgICAgICAgICAgICAgICAgICAkaGFuZGxlWzBdLmFkZEV2ZW50TGlzdGVuZXIoJ3RvdWNoY2FuY2VsJywgdGhpcy5ldmVudEhhbmRsZXIudG91Y2hlbmQsIHsgcGFzc2l2ZSA6IHRydWUgfSk7XG5cbiAgICAgICAgICAgICAgICAgICAgaWYoIU5TSU5LQV9HbG9iYWwuaXNfbW9iaWxlKCkpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICRoYW5kbGVbMF0uaXNNb3VzZURvd24gPSBmYWxzZTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgJC5lYWNoKFsnbW91c2Vkb3duJywgJ21vdXNlbW92ZScsICdtb3VzZXVwJ10sIGZ1bmN0aW9uKGksIGV2ZW50VHlwZSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICRoYW5kbGVbMF0uYWRkRXZlbnRMaXN0ZW5lcihldmVudFR5cGUsIF90aGlzLmV2ZW50SGFuZGxlci50b3VjaFNpbXVsYXRpb24sIHsgcGFzc2l2ZSA6IHRydWUgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgd2luZG93LmRpc3BhdGNoRXZlbnQobmV3IEV2ZW50KCdtb3VzZXVwJykpO1xuICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgfSxcbiAgICAgICAgICAgICAgICBldmVudEhhbmRsZXI6IHtcbiAgICAgICAgICAgICAgICAgICAgdG91Y2hzdGFydDogZnVuY3Rpb24oZSwgcHJldmVudFRoaXNQcm9wYWdhdGlvbikge1xuICAgICAgICAgICAgICAgICAgICAgICAgZS5zdG9wSW1tZWRpYXRlUHJvcGFnYXRpb24oKTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgdGhpcy5tYXhIZWlnaHQgPSAkKHRoaXMpLmNsb3Nlc3QoJ2FydGljbGUnKS53aWR0aCgpO1xuXG4gICAgICAgICAgICAgICAgICAgICAgICBpZihwcmV2ZW50VGhpc1Byb3BhZ2F0aW9uKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgICAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgICAgICAgICAgICAkKHRoaXMpLmFkZENsYXNzKCd0b3VjaHN0YXJ0Jyk7XG5cbiAgICAgICAgICAgICAgICAgICAgICAgIGlmKHR5cGVvZiBlLmRldGFpbCAhPSAndW5kZWZpbmVkJykge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmKHR5cGVvZiBlLmRldGFpbC5jaGFuZ2VkVG91Y2hlcyAhPSAndW5kZWZpbmVkJykge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBlLmNoYW5nZWRUb3VjaGVzID0gZS5kZXRhaWwuY2hhbmdlZFRvdWNoZXM7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgICAgICAgICAgICB0aGlzLnByZXZDbGllbnRYID0gdGhpcy50b3VjaHN0YXJ0UG9zaXRpb25YID0gKGUuY2hhbmdlZFRvdWNoZXMgPyAoZS5jaGFuZ2VkVG91Y2hlc1swXS5jbGllbnRYLXRoaXMuJGxheWVyLm9mZnNldCgpLmxlZnQpIDogMCk7XG4gICAgICAgICAgICAgICAgICAgICAgICB0aGlzLnRvdWNoc3RhcnRXaWR0aCA9ICQodGhpcykud2lkdGgoKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMucmVzaXN0YW5jZSA9IDEuMTtcblxuXG4gICAgICAgICAgICAgICAgICAgICAgICBjb25zb2xlLmxvZygndG91Y2hzdGFydFBvc2l0aW9uWCcsIHRoaXMudG91Y2hzdGFydFBvc2l0aW9uWCk7XG5cbiAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMuZGlyZWN0aW9uID0gdGhpcy5vcGVuZWQ/LTE6MTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgaWYoIXRoaXMub3BlbmVkKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgJC5lYWNoKHRoaXMud2hpbGVPcGVuQ2xvc2UsIGZ1bmN0aW9uKGksICRpdGVtKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICRpdGVtLnRyaWdnZXIoJ2Nsb3NlTGF5ZXInKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgICQodGhpcykucmVtb3ZlQ2xhc3MoJ29wZW4nKTtcblxuICAgICAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgICAgICB0b3VjaG1vdmU6IGZ1bmN0aW9uKGUpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGUuc3RvcEltbWVkaWF0ZVByb3BhZ2F0aW9uKCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAkKHRoaXMpLnJlbW92ZUNsYXNzKCd0b3VjaHN0YXJ0JykuYWRkQ2xhc3MoJ3RvdWNobW92ZScpO1xuXG4gICAgICAgICAgICAgICAgICAgICAgICBpZih0eXBlb2YgZS5kZXRhaWwgIT0gJ3VuZGVmaW5lZCcpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBpZih0eXBlb2YgZS5kZXRhaWwuY2hhbmdlZFRvdWNoZXMgIT0gJ3VuZGVmaW5lZCcpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZS5jaGFuZ2VkVG91Y2hlcyA9IGUuZGV0YWlsLmNoYW5nZWRUb3VjaGVzO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgIH1cblxuICAgICAgICAgICAgICAgICAgICAgICAgdmFyIGN1cnJlbnRDbGllbnRYID0gZS5jaGFuZ2VkVG91Y2hlc1swXS5jbGllbnRYLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNhbGMgPSBjdXJyZW50Q2xpZW50WC0odGhpcy50b3VjaHN0YXJ0UG9zaXRpb25YKjIpLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNhbGMgPSB0aGlzLnRvdWNoc3RhcnRXaWR0aCtjYWxjO1xuXG4gICAgICAgICAgICAgICAgICAgICAgICAvL2NhbGMgPSBlLmNoYW5nZWRUb3VjaGVzWzBdLmNsaWVudFg7XG4gICAgICAgICAgICAgICAgICAgICAgICB0aGlzLmRpcmVjdGlvbiA9IHRoaXMucHJldkNsaWVudFggPiBjdXJyZW50Q2xpZW50WCA/IC0xIDogMTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgLypcbiAgICAgICAgICAgICAgICAgICAgICAgIGlmKHRoaXMuZGlyZWN0aW9uICE9IHRoaXMucHJldkRpcmVjdGlvbikge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbnNvbGUubG9nKHRoaXMuZGlyZWN0aW9uLCB0aGlzLnByZXZEaXJlY3Rpb24pO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICAgICAgICAgICAgICAgIH0qL1xuXG4gICAgICAgICAgICAgICAgICAgICAgICBpZih0aGlzLiRsYXllci5hdHRyKCdpbnZlcnRUb3VjaCcpID09ICd0cnVlJykge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMuZGlyZWN0aW9uID0gdGhpcy5kaXJlY3Rpb24qLTE7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgY2FsYyA9IChjYWxjLXRoaXMudG91Y2hzdGFydFdpZHRoKSotMTtcbiAgICAgICAgICAgICAgICAgICAgICAgIH1cblxuICAgICAgICAgICAgICAgICAgICAgICAgdmFyIHRhcmdldFBvc2l0aW9uID0gdGhpcy5kaXJlY3Rpb24gPT09IDEgPyB0aGlzLm1heFdpZHRoIDogdGhpcy5taW5XaWR0aDtcblxuICAgICAgICAgICAgICAgICAgICAgICAgaWYoXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgKHRoaXMuZGlyZWN0aW9uID09PSAxICYmIGNhbGMgPj0gdGhpcy5tYXhXaWR0aCApIHx8XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgKHRoaXMuZGlyZWN0aW9uID09PSAtMSAmJiBjYWxjIDw9IHRoaXMubWluV2lkdGggKVxuICAgICAgICAgICAgICAgICAgICAgICAgKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgICAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgICAgICAgICAgICB0aGlzLiRsYXllci5jc3Moe1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICd0cmFuc2Zvcm0nOiAndHJhbnNsYXRlWCgnICsgY2FsYyArICdweCknXG4gICAgICAgICAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgdGhpcy5yZXNpc3RhbmNlID0gdGhpcy5yZXNpc3RhbmNlKi4xO1xuICAgICAgICAgICAgICAgICAgICAgICAgdGhpcy5wcmV2Q2xpZW50WCA9IGN1cnJlbnRDbGllbnRYO1xuXG4gICAgICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgICAgIHRvdWNoZW5kOiBmdW5jdGlvbihlKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAvL2UucHJldmVudERlZmF1bHQoKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIC8qXG4gICAgICAgICAgICAgICAgICAgICAgICBpZih0aGlzLmRpcmVjdGlvbiAhPSB0aGlzLnByZXZEaXJlY3Rpb24pIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLnByZXZEaXJlY3Rpb24gPSB0aGlzLmRpcmVjdGlvbjtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICAgICAgICAgICAgICB9Ki9cblxuICAgICAgICAgICAgICAgICAgICAgICAgdGhpcy5wcmV2RGlyZWN0aW9uID0gdGhpcy5kaXJlY3Rpb247XG5cbiAgICAgICAgICAgICAgICAgICAgICAgIGUuc3RvcEltbWVkaWF0ZVByb3BhZ2F0aW9uKCk7XG4gICAgICAgICAgICAgICAgICAgICAgICB0aGlzLiRsYXllci5yZW1vdmVDbGFzcygndG91Y2hzdGFydCcpLnJlbW92ZUNsYXNzKCd0b3VjaG1vdmUnKS5hZGRDbGFzcygndG91Y2hlbmQnKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMudG91Y2hzdGFydFBvc2l0aW9uWCA9IDA7XG5cbiAgICAgICAgICAgICAgICAgICAgICAgIGlmKHRoaXMuZGlyZWN0aW9uID09PSAxKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgdGhpcy4kbGF5ZXIucGFyZW50KCkudHJpZ2dlcignbGF5ZXItb3BlbmVkJyk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIGZpbmFsV2lkdGggPSB0aGlzLm1heFdpZHRoO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMub3BlbmVkID0gdHJ1ZTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLiRsYXllckNvbnRlbnQudHJpZ2dlcignbW92ZVRyYW5zaXRpb25TdGF0dXMnLCAxMDApO1xuICAgICAgICAgICAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLiRsYXllci5wYXJlbnQoKS50cmlnZ2VyKCdsYXllci1jbG9zZWQnKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YXIgZmluYWxXaWR0aCA9IHRoaXMubWluV2lkdGg7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgdGhpcy5vcGVuZWQgPSBmYWxzZTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLiRsYXllckNvbnRlbnQudHJpZ2dlcignbW92ZVRyYW5zaXRpb25TdGF0dXMnLCAwKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgIHZhciBfdGhpcyA9IHRoaXM7XG4gICAgICAgICAgICAgICAgICAgICAgICB0aGlzLiRsYXllci5jc3Moe1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICd0cmFuc2Zvcm0nOiAndHJhbnNsYXRlWCgnICsgZmluYWxXaWR0aCArICdweCknXG4gICAgICAgICAgICAgICAgICAgICAgICB9KS5vbmUodHJhbnNpdGlvbkVuZFRyaWdnZXIsIGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICQodGhpcykucmVtb3ZlQ2xhc3MoJ3RvdWNoZW5kJyk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYoX3RoaXMub3BlbmVkKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICQodGhpcykuYWRkQ2xhc3MoJ29wZW4nKTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgJCh3aW5kb3cpLnRyaWdnZXIoJ2xheWVyQ2hhbmdlJyk7XG4gICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICQod2luZG93KS50cmlnZ2VyKCdsYXllckNoYW5nZScpO1xuXG4gICAgICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgICAgIHRvdWNoU2ltdWxhdGlvbjogZnVuY3Rpb24oZSkge1xuICAgICAgICAgICAgICAgICAgICAgICAgdmFyIHR5cGUgPSBmYWxzZTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgc3dpdGNoIChlLnR5cGUpXG4gICAgICAgICAgICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgY2FzZSBcIm1vdXNlZG93blwiOlxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLmlzTW91c2VEb3duID0gdHJ1ZTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdHlwZSA9IFwidG91Y2hzdGFydFwiO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBicmVhaztcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICBjYXNlIFwibW91c2Vtb3ZlXCI6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGlmKHRoaXMuaXNNb3VzZURvd24pIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHR5cGUgPSBcInRvdWNobW92ZVwiO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdHlwZSA9IGZhbHNlO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNhc2UgXCJtb3VzZXVwXCI6XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRoaXMuaXNNb3VzZURvd24gPSBmYWxzZTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdHlwZSA9IFwidG91Y2hlbmRcIjtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYodGhpcyA9PSB3aW5kb3cpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHdpbmRvdy5jdXJyZW50VG91Y2hlZEVsZW0uZGlzcGF0Y2hFdmVudChuZXcgRXZlbnQoJ3RvdWNoZW5kJykpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgLy8kKHdpbmRvdy5jdXJyZW50VG91Y2hlZEVsZW0pLnRyaWdnZXIoJ3RvdWNoZW5kJyk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgZGVmYXVsdDogcmV0dXJuO1xuICAgICAgICAgICAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgICAgICAgICAgICBpZih0eXBlKSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgd2luZG93LmN1cnJlbnRUb3VjaGVkRWxlbSA9IHRoaXM7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyIHNpbXVsYXRlZEV2ZW50ID0gbmV3IEN1c3RvbUV2ZW50KHR5cGUsIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZm9vOiAnZGV2JyxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZGV0YWlsOiB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBjaGFuZ2VkVG91Y2hlczogW3tcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBjbGllbnRYIDogZS5jbGllbnRYLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNsaWVudFkgOiBlLmNsaWVudFlcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1dXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHNpbXVsYXRlZEV2ZW50LnRlc3QgPSAndGVzdCc7XG5cbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzLmRpc3BhdGNoRXZlbnQoc2ltdWxhdGVkRXZlbnQpO1xuICAgICAgICAgICAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICByZXR1cm47XG4gICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgIH1cblxuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgfSxcbiAgICAgICAgdGFibGV0OiB7XG4gICAgICAgICAgICBpbml0OiBmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICBpZighdGhpcy5pc0luaXRlZCkge1xuICAgICAgICAgICAgICAgICAgICB0aGlzLmlzSW5pdGVkID0gdHJ1ZTtcbiAgICAgICAgICAgICAgICAgICAgdGhpcy5wcmVwYXJlRGF0YSgpO1xuICAgICAgICAgICAgICAgICAgICB0aGlzLnNjcm9sbFN5bmMoKTtcbiAgICAgICAgICAgICAgICAgICAgdGhpcy5yYW5nZSgpO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICBwcmVwYXJlRGF0YTogZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgJCgnLmNocm9uaWNsZV9fbGlzdCcpLmVhY2goZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICAgICAgICAgIHZhciAkY2hyb25pY2xlTGlzdCA9ICQodGhpcyksXG4gICAgICAgICAgICAgICAgICAgICAgICBkYXRhID0ge307XG5cbiAgICAgICAgICAgICAgICAgICAgJCgnZHQnLCB0aGlzKS5lYWNoKGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICAgICAgICAgdmFyICR0aGlzID0gJCh0aGlzKSxcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAkZGQgPSAkdGhpcy5uZXh0KCdkZCcpLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRoaXNZZWFyID0gJCh0aGlzKS50ZXh0KCkqMTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgZGF0YVt0aGlzWWVhcl0gPSB7fTtcblxuICAgICAgICAgICAgICAgICAgICAgICAgJGRkLmZpbmQoJy5jaHJvbmljbGVfX2xpc3RfX21vbnRoJykuZWFjaChmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YXIgJHRoaXMgPSAkKHRoaXMpLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzTW9udGggPSAkdGhpcy5kYXRhKCdtb250aCcpLFxuICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICB0aGlzVGVhc2VyID0gJHRoaXMuZGF0YSgndGVhc2VyJykgfHwgKCR0aGlzLnRleHQoKS5zdWJzdHIoMCwgMjAwKSsnIOKApicpO1xuXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgaWYodHlwZW9mIGRhdGFbdGhpc1llYXJdW3RoaXNNb250aF0gPT0gJ3VuZGVmaW5lZCcpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgZGF0YVt0aGlzWWVhcl1bdGhpc01vbnRoXSA9IFtdO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgIH1cblxuICAgICAgICAgICAgICAgICAgICAgICAgICAgIGRhdGFbdGhpc1llYXJdW3RoaXNNb250aF0ucHVzaCh7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIHRlYXNlciA6IHRoaXNUZWFzZXIsXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIGNvbnRlbnQgOiAkdGhpcy5odG1sKClcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgICAgICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgICAgICAgICAgICAgICBmb3IodmFyIGkgPSAxOyBpIDw9IDEyOyBpKyspIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICB2YXIgJG1vbnRoRGl2ID0gJCgnPGRpdiBjbGFzcz1cImNocm9uaWNsZV9fbGlzdF9fbW9udGggY2hyb25pY2xlX19saXN0X19tb250aC0tdGlueVwiPicpO1xuICAgICAgICAgICAgICAgICAgICAgICAgICAgICRkZC5hcHBlbmQoJG1vbnRoRGl2KTtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAkLmVhY2goZGF0YVt0aGlzWWVhcl1baV0sIGZ1bmN0aW9uKGksIGVudHJpZXMpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdmFyICRkb3QgPSAkKCc8ZGl2IGNsYXNzPVwiY2hyb25pY2xlX19saXN0X19tb250aF9fZG90XCI+Jyk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICRtb250aERpdi5hcHBlbmQoJGRvdCk7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgICAgIH0pO1xuXG4gICAgICAgICAgICAgICAgICAgIGNvbnNvbGUubG9nKGRhdGEpO1xuICAgICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSxcbiAgICAgICAgICAgIHNjcm9sbFN5bmM6IGZ1bmN0aW9uKCkge1xuICAgICAgICAgICAgICAgICQoJy5jaHJvbmljbGVfX2NvbnRhaW5lcicpLnNjcm9sbChmdW5jdGlvbigpIHtcbiAgICAgICAgICAgICAgICAgICAgdmFyICR0aGlzID0gJCh0aGlzKSxcbiAgICAgICAgICAgICAgICAgICAgICAgICRhcnRpY2xlID0gJHRoaXMuY2xvc2VzdCgnLmNvbnRlbnQnKSxcbiAgICAgICAgICAgICAgICAgICAgICAgICRjb250YWluZXJzID0gJGFydGljbGUuZmluZCgnLmNocm9uaWNsZV9fY29udGFpbmVyJykubm90KCR0aGlzKTtcblxuICAgICAgICAgICAgICAgICAgICAkY29udGFpbmVycy5zY3JvbGxMZWZ0KCR0aGlzLnNjcm9sbExlZnQoKSk7XG4gICAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgcmFuZ2U6IGZ1bmN0aW9uKCkge1xuXG4gICAgICAgICAgICAgICAgJCgnaW5wdXRbdHlwZT1cInJhbmdlXCJdJykucmFuZ2VzbGlkZXIoe1xuICAgICAgICAgICAgICAgICAgICBwb2x5ZmlsbDogZmFsc2UsXG5cbiAgICAgICAgICAgICAgICAgICAgLy9oYW5kbGVDbGFzczogJ2Nocm9uaWNsZV9fcmFuZ2VzbGlkZXJfX2hhbmRsZSdcbiAgICAgICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgIH1cbn1cbiJdfQ==