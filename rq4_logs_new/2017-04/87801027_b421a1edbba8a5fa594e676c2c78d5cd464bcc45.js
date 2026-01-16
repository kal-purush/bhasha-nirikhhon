/*
 * @Author: grove.liqihan
 * @Date: 2017-04-10 17:01:02
 * @Desc: 归一化服务
 */

var _ = require("lodash");
var cheerio = require('cheerio');
var extend = require("extend");
var Q = require("q");
var request = require("request");
var StyleChange = function ($$qrRecognitionServerApi,options) {
    var self = this;
    self.qrRecognitionServerApi = $$qrRecognitionServerApi;
    self.options = {
        whiteList: {
            // normal: []
            normal:['width','float','padding-right','transform','display','vertical-align','overflow','clear']
        }
    };
    self.imgArr = [];
    if (_.isObject(options)) {
        extend(true, self.options, options);
    }
};

StyleChange.prototype.normalize = function (content) {
    var self = this;
    var defer = Q.defer();
    var content = `<div>${content}</div>`;
    var $ = cheerio.load(content, {
        decodeEntities: false,
        normalizeWhitespace: true,
        xmlMode: false,
        withDomLvl1: true
    });
    var html = null;
    var element = $("*").first();
    try {
        self.recursionDom(element, $);
        if (self.imgArr.length === 0) {
            process.nextTick(function () {
                defer.resolve(content);
            })
        } else {
            var newArr = _.take(self.imgArr, 5).concat(_.takeRight(self.imgArr,5));
            request.post( self.qrRecognitionServerApi + "/isQRCode",{
                form: {
                    'imageUrls':JSON.stringify(newArr)
                }
            }, function(error, response, body){
                var results = JSON.parse(body);

                if (self.imgArr.length <=10) {
                    $('img').each(function(index, item){
                        if( results[index].isQRCode === true) {
                            $(item).remove();
                        }
                    })
                } else {
                    for (var index=0; index <5 ;index++) {
                        if( results[index].isQRCode === true) {
                            $('img').eq(index).remove();
                        }
                    }
                    for (var index = 1;index < 6 ;index++) {
                        if (results[10-index].isQRCode === true) {
                            $('img').eq(self.imgArr.length - index).remove();
                        }
                    }
                }
                html = $.html();
                self.imgArr.length = 0;
                defer.resolve(html);
                // return html;    
            })
        }
    } catch (e) {
        console.error("recursionDom failed ...");
        console.error(e);
        html = content;        
        defer.resolve(html);
    }
    
    return defer.promise;
};

StyleChange.prototype.recursionDom = function (element, $) {
    var self = this;
    self.removeInvalidTags(element, $);
    self.removeStyleAttr(element);
    if (element.children().length > 0) {
        _.each(element.children(), function (value, key) {
            var $element = $(value);
            self.recursionDom($element, $);
            self.removeEmptyTag($element, $);
        })
    }
    if (element.children().length === 0) {
        self.removeEmptyNbsp(element);
    }
    return element;
};

//置空只含有&nbsp;的标签
StyleChange.prototype.removeEmptyNbsp = function (element) {
    var self = this;
    if (element.length === 0) {
        return;
    }
    if (/[&nbsp;\s]+/g.test(element.html())) {
        element.empty();
    }
    if (/[二维码\s]+/g.test(element.html())) {
        element.empty();
    }
};

//精简嵌套
StyleChange.prototype.removeInvalidTags = function (element, $) {
    var self = this;
    if (element.length === 0) {
        return;
    }
    var firstElement = _.first(element);
    var firstElementTagName = firstElement.tagName.toLowerCase();
    var children = firstElement.childNodes;
    if (children.length === 1) {
        var firstChild = _.first(children);
        if (_.isString(firstChild.tagName)) {
            var childTagName = firstChild.tagName.toLowerCase();
            if (childTagName === firstElementTagName) {
                element.empty().append($(firstChild).html());
                self.removeInvalidTags(element, $);
            }
        }
    }
};

//删除br的
StyleChange.prototype.removeEmptyTag = function (element) {
    var self = this;
    //新增去除文章两边空格的代码
    element.html(_.trim(element.html()));
    // element[0].innerHTML = _.trim(element[0].innerHTML);
    var tagName = element[0].tagName.toLowerCase();
    // var elementStyle = element[0].style._values;
    //删除br
    var empty = [
        'span',
        'div',
        'section',
        'p',
        'strong',
        'td',
        'tr',
        'pre',
        'tbody',
        'table',
        'h1'
    ];

    if (tagName == 'br') {
        element.remove();
    }
    if (tagName == 'hr') {
        element.remove();
    }
    if (tagName == 'img') {
        if (!element.attr("src")) {
            element.remove();
            return;
        }
        self.imgArr.push(element.attr("src"));
        if (!element.data("bdttSrc")) {
            element.attr("data-bdtt-src", element.attr("src"));
            element.attr("src", "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsQAAA7EAZUrDhsAAAANSURBVBhXYzh8+PB/AAffA0nNPuCLAAAAAElFTkSuQmCC")
        }
    }
    if (empty.indexOf(tagName) >= 0 && _.isEmpty(element.html())) {
        //如果为display:inline-block并且没有宽度时，删除标签
        if (element.css("display") === "inline-block") {
            var width = element.css("width");
            if (!width || width === "0px" || width === 0 || width === "0") {
                element.remove();
            }
        }
        else {
            //标签若没有清除浮动
            if (!element.css('clear')) {
                //正常空标签删除标签
                element.remove();
            }
        }
    }
    if (tagName == 'a') {
        element.removeAttr("href");
    }
};

StyleChange.prototype.removeStyleAttr = function (element) {
    var self = this;
    if (element.length === 0) {
        return;
    }
    var newStyles = {};
    var tagName = element[0].tagName.toLowerCase();
    var list = self.options.whiteList.normal.concat(self.options.whiteList[tagName]);
    _.each(element.css(), function(value, key) {
        if (list.indexOf(key) >= 0) {
            newStyles[key] = value;
        }
    });

    if (element.css("display") === "inline-block" || element.css("float")) {
        newStyles["margin-bottom"] = "0px!important;";
    } else {
        var parent = element.parent();
        if (parent.length == 1 && (parent.css("display") === "inline-block" || parent.css("float"))) {
             newStyles["margin-bottom"] = "0px!important;";
        }
    }
    element.removeAttr('style');
    _.each(element.css(), function(value, key) {
        element.css(key, null);
    });
    element.css(newStyles);
};

module.exports = StyleChange;
