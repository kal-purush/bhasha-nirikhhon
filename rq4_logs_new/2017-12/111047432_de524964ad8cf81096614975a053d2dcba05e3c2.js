var lib = {
    ctx: '',
    cache: {},
    log: function(m) {
        if (is.object(console) && is.fn(console.log)) {
            console.log(m);
        }
    },
    getURL: function(data) {
        var result = decodeURIComponent($.param(data));
        return result;
    },
    getQuery: function(name, defaultValue) {
        var query = null;
        var search = window.location.search.substring(1); //remove ?

        var args = search.split('&');
        for (var i = 0; i < args.length; i++) {
            var items = args[i].split('=');
            if (items[0].toLocaleLowerCase() == name.toLocaleLowerCase()) {
                if (query == null) {
                    query = decodeURI(items[1]);
                } else {
                    query = ',' + decodeURI(items[1]);
                }
            }
        }

        return query ? query : defaultValue;
    },
    format: function() {
        var s = arguments[0];
        for (var i = 0; i < arguments.length - 1; i++) {
            var reg = new RegExp("\\{" + i + "\\}", "gm");
            s = s.replace(reg, arguments[i + 1] ? arguments[i + 1] : '');
        }

        return s;
    },
    map: function(item, viewModel) {
        for (var i in item) {
            try {
                var t = typeof(viewModel[i])
                if (t == 'function') {
                    if (typeof(item[i]) != 'undefined') {
                        viewModel[i](item[i]);
                    } else {
                        viewModel[i]('');
                    }
                } else if (t != 'undefined') {
                    viewModel[i] = item[i]
                }
            } catch (ex) {
                // console.log()
            }

        }
    },
    unmap: function(viewModel) {
        var item = {};
        for (var i in viewModel) {
            try {
                if (ko.isObservable(viewModel[i])) {
                    item[i] = viewModel[i]();
                } else if (typeof(viewModel[i]) != 'function') {
                    item[i] = viewModel[i];
                }
            } catch (ex) {
                // console.log()
            }
        }
        return item;
    },
    clearmap: function(viewModel) {
        for (var i in viewModel) {
            try {
                if (typeof(viewModel[i]) == 'function') {
                    viewModel[i]('');
                } else {
                    viewModel[i] = '';
                }
            } catch (ex) {
                // console.log()
            }
        }
    },
    redirect: function(url, delay) {
        if (url == undefined || url == null) {
            url = is.mobile() ? '/' : '/m';
        }
        var go = function() {
            return function() {
                var ua = navigator.userAgent.toLowerCase(),
                    isIE = ua.indexOf('msie') !== -1,
                    version = parseInt(ua.substr(4, 2), 10);

                // Internet Explorer 8 and lower
                if (isIE && version < 9) {
                    $('a').attr('href', url).appendTo($('body')).trigger('click');
                } else { // All other browsers
                    window.location.href = url;
                    // window.location.replace(url);
                }
            }
        }();

        if (delay) {
            window.setTimeout(function() {
                go();
            }, delay);
        } else {
            go();
        }

    },
    hasValue: function(s) {
        return typeof(s) != 'undefined' && s != null && s != '';
    },
    openWin: function(src, h, w, name) {
        name = name || 'newwindow';
        var iTop = (window.screen.availHeight - 30 - h) / 2;
        var iLeft = (window.screen.availWidth - 10 - w) / 2;
        return window.open("", name, "width=" + w + ", height=" + h + ",top=" + iTop + ",left=" + iLeft + ",toolbar=no, menubar=no, scrollbars=no, resizable=no,location=no, status=no,alwaysRaised=yes,depended=yes");
    },

    //#region ajax
    get: function(api, data, success, opts) {
        return lib.ajax('GET', api, data, success, opts);
    },
    post: function(api, data, success, opts, files) {
        return lib.ajax('POST', api, data, success, opts, files);
    },
    put: function(api, data, success, opts) {
        return lib.ajax('PUT', api, data, success, opts);
    },
    del: function(api, data, success, opts) {
        return lib.ajax('DELETE', api, data, success, opts);
    },
    ajax: function(type, api, data, success, opts, files) {
        var caller = arguments.callee.caller.caller.toString();
        if (lib.cache.hasOwnProperty(type + api + caller)) {
            return;
        } else {
            lib.cache[type + api + caller] = 'calling';
        }
        var url = lib.ctx + api;
        if ((type == "DELETE" || type == "GET") && data) {
            if (url.indexOf('?') > -1) {
                url += '&' + $.param(data);
            } else {
                url += '?' + $.param(data);
            }
            data = {}
        }

        var mute = opts && opts.context && opts.context.mute;

        o = $.extend({
            type: type,
            url: url,
            data: data,
            success: function(data, textStatus, xhr) {
                $('body').data('loading.stop', false);

                if (xhr.status == 204 && !mute) {
                    if (!is.mobile()) {
                        $('#main-loading-mask').show();
                        window.setTimeout(function() {
                            $('.page_link').show();
                        }, 3000);
                    }
                    lib.redirect(xhr.getResponseHeader("Location"));
                    return;
                }

                if (is.object(data)) {
                    if (!mute) {
                        if (data.Error) {
                            if (is.fn(this.Error)) {
                                this.Error(data.Error);
                            } else {
                                lib.showError(data.Error);
                            }

                            return
                        } else if (data.Warn) {
                            if (is.fn(this.Warn)) {
                                this.Warn(data.warn);
                            } else {
                                lib.showWarning(data.warn);
                            }
                            return
                        }
                    }
                }

                if (is.fn(success)) {
                    success(data)
                    rebind();
                }
            },
            timeout: 1000 * 10 * 60,
            dataType: 'json',
            converters: {
                'text json': function(result) {
                    if (result === "") result = null; //fixed parseerror when statuscode is 204
                    return jQuery.parseJSON(result);
                }
            },
            complete: function(xhr, textStatus) {
                delete lib.cache[type + api + caller];
                if (xhr.status == 312) {
                    lib.redirect(xhr.getResponseHeader("Location"));
                } else if (xhr.status == 301 || xhr.status == 302) {
                    lib.redirect(xhr.getResponseHeader("Location"));
                } else if (xhr.status != 200) {
                    //+ xhr.responseText
                    //lib.showError('[' + xhr.status + ']' + xhr.statusText);
                }

                if (this.loading === false) {
                    return;
                } else {
                    try {
                        $('body').overlayout();
                    } catch (error) {

                    }
                }

                $('body').data('loading.title', '');
                $('body').data('loading.stop', false);
            },
            error: function(xhr, textStatus, m) {
                $('body').data('loading.stop', false)
                if (!mute) {
                    if (textStatus === "timeout") {
                        lib.showWarning('请求超时,请重试?')
                    } else {
                        if (xhr.status == 401) {
                            var m = xhr.responseText;
                            lib.showError('[' + xhr.status + '' + xhr.statusText + ']' + xhr.responseText);
                        } else if (xhr.status == 0) {
                            // lib.showError('服务器未响应，请稍后再试?');
                        }

                    }
                }

            }
        }, opts || {});

        var f = files || ko.files;

        o.files = [];
        $.each(f, function() {
            if ($(this).val()) {
                o.iframe = true;
                o.processData = false;
                o.files.push($(this).get(0));
            }
        });

        return $.ajax(o).fail(function(xhr, textStatus) {
            if (mute) {
                return;
            }

            var text = xhr.responseText;
            if (text !== "") {
                try {
                    var data = jQuery.parseJSON(text);
                    if (is.object(data)) {
                        if (data.Error) {
                            lib.showError(data.Error);
                            return
                        } else if (data.Warn) {
                            lib.showWarning(data.Warn);
                            return
                        }
                    }
                } catch (ex) {
                    // if (xhr.status == 502 || xhr.status == 503 || xhr.status == 504) {
                    lib.showError("网络超时，请重试");
                    // }
                }
            } else {
                switch (xhr.status) {
                    case 312:
                    case 569:
                        if (!is.mobile()) {
                            $('#main-loading-mask').show();
                            window.setTimeout(function() {
                                $('.page_link').show();
                            }, 3000);
                        }
                        lib.redirect(xhr.getResponseHeader("Location"));
                        break;
                    case 301:
                    case 302:
                        lib.redirect(xhr.getResponseHeader("Location"));
                        break;
                    default:
                        // lib.showError('[' + xhr.status + ']' + textStatus);
                        break;
                }
            }
        });
    },
    //#endregion

    //#region loading
    showLoading: function(title) {
        if (typeof $.fn.overlay === "function") {
            title = title !== undefined ? title : "Loading...";
            $('body').overlay(title);
        }
    },
    hideLoading: function() {
        if (typeof $.fn.overlayout === "function") {
            $('body').overlayout();
        }
    },
    //#endregion

    //#region message
    focus: function(el) {
        if ($(el).is(':focusable')) {
            $(el).focus();
        } else {
            $('body').trigger('y2:focus');
        }
    },
    showInfo: function(msg, title, icon) {
        this.showNotify('info', msg, title, icon);
        alert(msg);
    },
    showSuccess: function(msg, title, icon) {
        alert(msg);
    },
    showWarning: function(msg, title, icon) {
        alert(msg);

    },
    showError: function(msg, title, icon) {
        alert(msg);
    },

    //#endregion

    //#region model
    applyBindings: function(viewModel, selector, init, extend) {
        viewModel.errors = ko.validation.group(viewModel);
        viewModel.valid = function() {
            var isValid = viewModel.errors().length == 0;

            if (isValid) {
                for (var i in viewModel) {
                    var item = viewModel[i];
                    if (typeof(item) == 'undefined') {
                        continue;
                    }
                    if (is.fn(item.valid)) {
                        if (item.valid() == false) {
                            isValid = false;
                            item.errors.showAllMessages();
                        }
                    } else if (is.array(item)) {
                        for (var n in item) {
                            var child = item[n];
                            if (is.fn(child.valid)) {
                                if (child.valid() == false) {
                                    isValid = false;
                                    child.errors.showAllMessages();
                                }
                            }
                        }
                    } else if (ko.isObservableArray(item)) {
                        var items = item();
                        for (var m in items) {
                            var oa = items[m];
                            if (is.fn(oa.valid)) {
                                if (oa.valid() == false) {
                                    isValid = false;
                                    oa.errors.showAllMessages();
                                }
                            }
                        }
                    }
                }
            }

            if (viewModel.errors().length === 0 && isValid) {
                return true;
            } else {
                viewModel.errors.showAllMessages();
                setTimeout(function() {
                    $('.has-error:focusable:first').focus();
                }, 0);

                return false;
            }
        };
        viewModel.val = function() {
            return lib.unmap(viewModel);
        };
        viewModel.reset = function() {
            lib.unmap(viewModel);
            viewModel.errors.showAllMessages(false);
        };

        if (is.fn(extend)) {
            extend(viewModel);
        }

        if ($(selector).size() > 0) {
            $(selector).each(function() {
                ko.applyBindings(viewModel, $(this).get(0));
            });

            if (is.fn(init)) {
                init(viewModel);
            }
        }

        return viewModel;
    },

    //#endregion

    //#region applyPartialBindings
    applyPartialBindings: function(viewModel, selectors, init, extend) {
        viewModel.errors = ko.validation.group(viewModel);
        viewModel.valid = function() {
            var isValid = viewModel.errors().length == 0;

            if (isValid) {
                for (var i in viewModel) {
                    var item = viewModel[i];
                    if (typeof(item) == 'undefined') {
                        continue;
                    }
                    if (is.fn(item.valid)) {
                        if (item.valid() == false) {
                            isValid = false;
                            item.errors.showAllMessages();
                        }
                    } else if (is.array(item)) {
                        for (var n in item) {
                            var child = item[n];
                            if (is.fn(child.valid)) {
                                if (child.valid() == false) {
                                    isValid = false;
                                    child.errors.showAllMessages();
                                }
                            }
                        }
                    } else if (ko.isObservableArray(item)) {
                        var items = item();
                        for (var m in items) {
                            var oa = items[m];
                            if (is.fn(oa.valid)) {
                                if (oa.valid() == false) {
                                    isValid = false;
                                    oa.errors.showAllMessages();
                                }
                            }
                        }
                    }
                }
            }

            if (viewModel.errors().length === 0 && isValid) {
                return true;
            } else {
                viewModel.errors.showAllMessages();
                setTimeout(function() {
                    $('.has-error:focusable:first').focus();
                }, 0);

                return false;
            }
        };
        viewModel.val = function() {
            return lib.unmap(viewModel);
        };
        viewModel.reset = function() {
            lib.unmap(viewModel);
            viewModel.errors.showAllMessages(false);
        };

        if (is.fn(extend)) {
            extend(viewModel);
        }

        var root = selectors.root;
        if (root != undefined && $(root).size() > 0) {
            $(root).each(function() {
                ko.applyBindings(viewModel, $(this).get(0));
            });

            if (is.fn(init)) {
                init(viewModel);
            }
        }

        var children = selectors.children;
        if (children != undefined && children.length > 0) {
            $.each(children, function(index, value) {
                $(value).on('bindMe', function() {
                    if (!$(this).data('hasbind')) {
                        ko.applyBindings(viewModel, $(this).get(0));
                        $(this).data('hasbind', true)
                    }
                });
            });

            var firstChild = children[0];
            if ($(firstChild).size() > 0) {
                ko.applyBindings(viewModel, $(firstChild).get(0));
                $(firstChild).data('hasbind', true)
            }
        }

        return viewModel;
    },
    //#endregion

    //#region delay
    delay: function(sec, fn) {
        window.setTimeout(fn, sec * 1000);
    },
    //#endregion

    //#region Array
    removeArrayItem: function(arr, item) {
        for (var i = 0; i < arr.length; i++) {
            if (arr[i] == item) {
                arr.splice(i, 1);
                break;
            }
        }
    },
    //#endregion

    //#region toDateTimeString
    toDateTimeString: function(d) {
        if (is.datetime(d)) {
            var y = d.getFullYear();
            var m = d.getMonth() + 1;
            var day = d.getDate();
            var h = d.getHours();
            var mi = d.getMinutes();
            var s = d.getSeconds();
            return this.pad(y, 4, '0') + '-' + this.pad(m, 2, '0') + '-' + this.pad(day, 2, '0') + ' ' + this.pad(h, 2, '0') + ':' + this.pad(mi, 2, '0') + ':' + this.pad(s, 2, '0');
        }

        return '';
    },
    //#endregion

    //#region getUTCTime
    getUTCTime: function(timezone) {
        var localTime = new Date();
        localTime.setMinutes(localTime.getMinutes() + localTime.getTimezoneOffset() + timezone * 60);

        return localTime;
    },
    //#endregion

    //#region toDateTimeString
    pad: function(v, length, left) {
        if (v == null || typeof(v) == 'undefined') {
            v = ''
        }

        v = v.toString();
        left = left.toString();
        for (l = v.length; l < length; l++) {
            v = left + v;
        }

        return v;
    },
    //#endregion

    //#region toFixed
    toFixed: function(v, num) {
        var f = Number(v);
        if (isNaN(f)) {
            return v;
        } else {
            var n = Number(num);
            if (isNaN(n)) {
                return f;
            } else {
                return f.toFixed(n);
            }
        }
    },
    //#endregion

    //#region toFloat
    toFloat: function(v, d) {
        var f = parseFloat(v);
        if (isNaN(f)) {
            return d;
        } else {
            return f;
        }
    },
    //#endregion

    //#region toInt
    toInt: function(v, d) {
        var f = parseInt(v);
        if (isNaN(f)) {
            return d;
        } else {
            return f;
        }
    },
    //#endregion

    //#region filterInt
    filterInt: function(v) {
        if (/^(\-|\+)?([0-9]+|Infinity)$/.test(v))
            return Number(v);
        return NaN;
    },
    //#endregion

    //#region getCookie
    getCookie: function(name) {
        var value = "; " + document.cookie;
        var parts = value.split("; " + name + "=");
        if (parts.length == 2) return parts.pop().split(";").shift();
    },
    //#endregion

    //#region fixPopupCenter
    gamesPopupCenter: function(url, title, w, h) {
        // Fixes dual-screen position
        var dualScreenLeft = window.screenLeft != undefined ? window.screenLeft : screen.left;
        var dualScreenTop = window.screenTop != undefined ? window.screenTop : screen.top;

        var width = window.innerWidth ? window.innerWidth : document.documentElement.clientWidth ? document.documentElement.clientWidth : screen.width;
        var height = window.innerHeight ? window.innerHeight : document.documentElement.clientHeight ? document.documentElement.clientHeight : screen.height;

        var left = ((width / 2) - (w / 2)) + dualScreenLeft;
        var top = ((height / 2) - (h / 2)) + dualScreenTop;
        var newWindow = window.open(url, title, 'resizable=yes,toolbar=no,location=no,directories=no,status=no,menubar=no,scrollbars=no, width=' + w + ', height=' + h + ', top=' + top + ', left=' + left);
        return newWindow;
    },
    //#endregion

    //#region SB_Sport
    sb_sport: {
        //讓球
        Team_hdp_transfer: function(bet_type, hdp) {
            //bet_type == 讓分盤
            switch (bet_type) {
                case 1:
                case 7:
                case 17:
                    return this.hdp_transfer(hdp);
                    break;

                    //虛擬 賽馬/賽車/賽狗(沒有VS)
                case 1231:
                    return "";
                    break;

                default:
                    return "VS";
                    break;
            }
        },
        DaXiao_hdp_transfer: function(bet_type, hdp) {
            //bet_type == 大小盤
            switch (bet_type) {
                case 3:
                case 8:
                case 18:
                case 81:
                case 82:
                case 85:
                case 144:
                case 156:
                case 178:
                case 197:
                case 198:
                case 204:
                case 205:
                case 401:
                case 402:
                case 403:
                case 404:
                case 610:
                case 615:
                case 616:
                case 1203:
                case 1306:
                    return this.hdp_transfer(hdp);
                    break;

                default:
                    return "";
                    break;
            }
        },
        hdp_transfer: function(hdp) {
            //hdp結尾 (.25 or .75) => hdp - 0.25 / hdp + 0.25
            //e.g. hdp = 1.25 => 1 / 1.5
            //hdp結尾非 (.25 or .75) 直接 hdp
            var hdp_string = String(Math.abs(hdp));
            var hdp_number = new Number(Math.abs(hdp));
            var hdp_string_split = hdp_string.split(".")[1];
            if (hdp_string_split == "25" || hdp_string_split == "75") {
                return (hdp_number - 0.25) + "/" + (hdp_number + 0.25);
            } else {
                return hdp_number;
            }
        }
    },
    //#endregion
    //#region BBIN
    BBIN: {
        //莊閒
        ZhuangXian_formatting: function(result, producttype) {
            var banker = "";
            var player = "";
            var result_array = [];

            if (producttype == "1353") {
                banker = "<span class='text-red'>庄</span>（牛" + result.banker.result + "）";
            } else {
                banker = "<span class='text-red'>庄</span>（" + result.banker.result + "）";
            }

            result_array.push({
                name: banker,
                value: result.banker.cards
            });

            //檢查 閒有幾組
            if (result.player.length > 1) {
                $.each(result.player, function(i, v) {
                    var string = v.result;
                    switch (producttype) {
                        //1347 三公
                        case "1347":
                            //P=单公, 2P=双公
                            string = string.replace("2P", "双公");
                            string = string.replace("P", "单公");
                            break;

                            //1353 牛牛
                        case "1353":
                            if (parseInt(string) != 0) {
                                string = "牛" + string;
                            } else {
                                string = "无牛";
                            }
                            break;

                        default:
                            string;
                            break;
                    }
                    player = "<span class='text-blue'>闲" + (i + 1) + "</span>（" + string + "）";
                    result_array.push({
                        name: player,
                        value: v.cards
                    });
                });
            } else {
                player = "<span class='text-blue'>闲</span>（" + result.player[0].result + "）";
                result_array.push({
                    name: player,
                    value: result.player[0].cards
                });
            }

            return result_array;
        },
        //龍虎鬥
        LongHu_formatting: function(result) {
            var result_array = [];

            var dragon = {
                name: "<span class='text-red'>龙</span>（" + result.dragon.result + "）",
                value: result.dragon.cards
            };
            result_array.push(dragon);

            var tiger = {
                name: "<span class='text-blue'>虎</span>（" + result.tiger.result + "）",
                value: result.tiger.cards
            };
            result_array.push(tiger);

            return result_array;
        },
        //溫州牌九 門
        Meng_formatting: function(result) {
            var banker = "";
            var result_array = [];
            banker = "<span class='text-red'>庄</span>";
            result_array.push({
                name: banker,
                value: result.banker.cards
            });

            //順門
            var shun = {
                name: "<span>顺门</span>" + lib.BBIN.WinLost_formatting(result.player[0].result),
                value: result.player[0].cards
            };
            result_array.push(shun);
            //出門
            var chu = {
                name: "<span>出门</span>" + lib.BBIN.WinLost_formatting(result.player[1].result),
                value: result.player[1].cards
            };
            result_array.push(chu);
            //到門
            var dao = {
                name: "<span>到门</span>" + lib.BBIN.WinLost_formatting(result.player[2].result),
                value: result.player[2].cards
            };
            result_array.push(dao);

            return result_array;
        },
        //二八槓
        MaJiang_formatting: function(result) {
            var banker = "";
            var result_array = [];
            banker = "<span class='text-red'>庄</span>（" + result.banker.result + "）";
            result_array.push({
                name: banker,
                value: result.banker.cards
            });

            //上門
            var shang = {
                name: "<span>上门</span>（" + result.player[0].result + "）",
                value: result.player[0].cards
            };
            result_array.push(shang);
            //中門
            var zhong = {
                name: "<span>中门</span>（" + result.player[1].result + "）",
                value: result.player[1].cards
            };
            result_array.push(zhong);
            //下門
            var xia = {
                name: "<span>下门</span>（" + result.player[2].result + "）",
                value: result.player[2].cards
            };
            result_array.push(xia);

            return result_array;
        },
        //德州撲克
        DeZhou_formatting: function(result) {
            var result_array = [];
            result_array.push({
                name: "庄",
                value: result.banker_card
            });
            result_array.push({
                name: "闲",
                value: result.player_card
            });
            result_array.push({
                name: "公家牌",
                value: result.board_card
            });
            result_array.push({
                name: "赢牌",
                value: result.winner_card
            });

            return result_array;
        },
        WinLost_formatting: function(result) {
            if (result == "W") {
                return "<span class='text-red'>赢</span>";
            } else {
                return "<span class='text-blue'>输</span>";
            }
        }
    },
    //endregion
    //#region Image(第三方圖片)
    Image_3rd: {
        image_folder: "../includes/img/white/member/",
        img_html: function(forder, filename) {
            return "<img src='" + lib.Image_3rd.image_folder + forder + filename + ".jpg' class='game_result_pic' width='36px' />";
        },
        //骰子
        ShaiZi_image: function(value) {
            var folder = "dice/";
            var filename = "";
            switch (value) {
                case "1":
                    filename = "D-1";
                    break;

                case "2":
                    filename = "D-2";
                    break;

                case "3":
                    filename = "D-3";
                    break;

                case "4":
                    filename = "D-4";
                    break;

                case "5":
                    filename = "D-5";
                    break;

                case "6":
                    filename = "D-6";
                    break;
            }
            return lib.Image_3rd.img_html(folder, filename);
        },
        //Poker
        Poker_image: function(value) {
            var folder = "poker/";
            var filename = "";
            var value_array = value.split('.');
            filename = value_array[0] + "-" + value_array[1];

            return lib.Image_3rd.img_html(folder, filename);
        },
        //Poker2
        Poker2_image: function(value) {
            var folder = "poker/";
            var filename = value;
            return lib.Image_3rd.img_html(folder, filename);
        },
        //魚蝦蟹
        YuXaXie_image: function(value) {
            var folder = "dice2/";
            var filename = "";
            switch (value) {
                case "1":
                    filename = "F-1";
                    break;

                case "2":
                    filename = "S-1";
                    break;

                case "3":
                    filename = "G-1";
                    break;

                case "4":
                    filename = "M-1";
                    break;

                case "5":
                    filename = "C-1";
                    break;

                case "6":
                    filename = "CK-1";
                    break;
            }
            return lib.Image_3rd.img_html(folder, filename);
        },
        //麻將
        MaJiang_image: function(value) {
            var folder = "mj/";
            var filename = "";
            var tmp = value.split(".");
            filename = "MJ-" + tmp[1];

            return lib.Image_3rd.img_html(folder, filename);
        },
        //色蝶
        SeDie_image: function(value) {
            var folder = "disk/";
            var filename = "";
            if (value === 'w') {
                filename = "CR-W";
            } else if (value === 'r') {
                filename = "CR-R";
            }

            return lib.Image_3rd.img_html(folder, filename);
        }
    },
    //endregion
};