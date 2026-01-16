$(function () {
    window.selfTool = function () {
        var reg = new RegExp("\\[([^\\[\\]]*?)\\]", 'igm');
        var alr = $("#comAlert");
        var ahtml = alr.html();

        var _tip = function (options, sec) {

            // 弹出如果是纯文本
            if (typeof options === 'string') {

                options = {
                    title: '错误提醒',
                    msg: options,
                    center: true
                };
            }

            alr.html(ahtml);    // 复原
            alr.find('.ok').hide();
            alr.find('.cancel').hide();
            alr.find('.modal-content').width(500);
            _dialog(options, sec);


            return {
                on: function (callback) {

                }
            };
        };

        var _alert = function (options) {
            // 弹出如果是纯文本
            if (typeof options === 'string') {

                options = {
                    Title: '错误提醒',
                    Message: options,
                    BtnOk: '确认',
                    center: true
                };
            }

            if (!options.BtnOk)  options.BtnOk = '确认';
            alr.html(ahtml);  // 复原
            alr.find('.ok').removeClass('btn-success').addClass('btn-primary');
            alr.find('.cancel').hide();
            _dialog(options);

            return {
                on: function (callback) {
                    if (callback && callback instanceof Function) {
                        alr.find('.ok').click(function () { callback(true) });
                    }
                }
            };
        };

        var _confirm = function (options) {
            alr.html(ahtml); // 复原
            alr.find('.ok').removeClass('btn-primary').addClass('btn-success');
            alr.find('.cancel').show();
            _dialog(options);

            return {
                on: function (callback) {
                    if (callback && callback instanceof Function) {
                        alr.find('.ok').click(function () { callback(true) });
                        alr.find('.cancel').click(function () { return; });
                    }
                }
            };
        };

        var _dialog = function (options) {
            var ops = {
                msg: !options.Message ? "提示内容" : options.Message,
                title: !options.Title ? "提示内容" : options.Title,
                btnok: "确定",
                btncl: "取消",
                width: !options.width ? '300px' : options.width,
                height: !options.height ? 'auto' : options.height,
                okBtnId: !options.okBtnId ? 'okBtnId' : options.okBtnId
            };

            // 大小都是auto 给默认

            $.extend(ops, options);

            if (options.center === true) {
                alr.addClass('in')
                    .attr('aria-hidden', false)
                    .css({
                        left: '35%',
                        top: '35%',
                        margin: "0 auto",
                        background: 'none'
                    });
                alr.find('.modal-dialog').css({
                    margin: 0,
                    padding: 0
                });
            }

            alr.find('.modal-dialog').css({
                width: ops.width,
                height: ops.height
            });

            var html = alr.html().replace(reg, function (node, key) {
                var opt = {
                    Title: ops.title,
                    BtnOk: ops.btnok,
                    BtnCancel: ops.btncl,
                    okBtnId: options.okBtnId,
                    Message: ops.Message
                };
                return opt[key];
            });

            alr.html(html);
            alr.modal({
                backdrop: 'static'
            });
            // 遮罩 ZIndex
            jQuery('.modal-backdrop:last').css({zIndex: 9998});
        };



        return {
            tip: _tip,
            alert: _alert,
            confirm: _confirm
        }

    }();
});