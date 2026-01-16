///<jscompress sourcefile="content.js" />
/**
 * Created by Dovahkiin on 2017/4/3.
 */
var jData = []//使用JSON.stringify(jData)将数据转化为json格式
var Content = function () {
    this.id = ('Cont_' + Math.random()).replace(".", "_")
    this.el = $('<div class="Content" id="' + this.id + '"></div>').hide()
    this.page = []
    $("body").append(this.el)
    this.addPage = function (name, text) {
        jData.push({isPage: true, name: name, text: text})
        var page = $('<div class="page section"></div>')
        if (name != undefined) {
            page.addClass('page_' + name)
        }
        if (text != undefined) {
            page.html(text)
        }
        this.el.append(page)
        this.page.push(page)
        if (typeof this.whenAddPage === 'function') {
            this.whenAddPage()
        }
        return this;
        return this
    }
    this.addComponent = function (name, cfg) {
        jData.push({isPage: false, name: name, cfg: cfg})
        var cfg = cfg || {}
        cfg = $.extend({
            type: 'base'
        }, cfg)
        var component
        var page = this.page.slice(-1)[0]
        switch (cfg.type) {
            case 'base':
                component = new ComponentBase(name, cfg)
                break

            case 'polyline' :
                component = new ComponentPolyline(name, cfg);
                break;

            case 'pie' :
                component = new ComponentPie(name, cfg);
                break;
            case 'bar' :
                component = new ComponentBar(name, cfg);
                break;
            case 'bar_v' :
                component = new ComponentBar_v(name, cfg);
                break;

            case 'radar' :
                component = new ComponentRadar(name, cfg);
                break;

            case 'pie' :
                component = new ComponentPie(name, cfg);
                break;
            case 'ring' :
                component = new ComponentRing(name, cfg);
                break;
            case 'point' :
                component = new ComponentPoint(name, cfg);
                break;
            default:
        }
        page.append(component)
        return this
    }
    this.loader = function (firstPage) {
        this.el.fullpage({
                onLeave: function (index, nextIndex, dircetion) {
                    $(this).find('.component').trigger("onLeave")
                },
                afterLoad: function (anchorLink, index) {
                    $(this).find('.component').trigger("afterLoad")
                }
            }
        )
        this.page[0].find('component').trigger("afterLoad")
        this.el.show()
        if (firstPage) {
            $.fn.fullpage.moveTo(firstPage);
        }
    }
    this.loader = typeof Content_loading == 'function' ? Content_loading : this.loader;

    return this
}

///<jscompress sourcefile="loading.js" />
/**
 * Created by Dovahkiin on 2017/4/7.
 */
var Content_loading = function (images, firstPage) {
    var id = this.id
    if (this._images === undefined) {
        this._images = (images || []).length
        this._load = 0
        window[id] = this
        for (var s in images) {
            var item = images[s]
            var img = new Image
            img.onload = function () {
                window[id].loader();
            }
            img.src = item
        }
        $('#rate').text('0%')
        return this
    }
    else {
        this._load++
        $('#rate').text(((this._load / this._images * 100) >> 0) + '%')
        if (this._load < this._images) {
            return this
        }
    }
    window[id] = null
    this.el.fullpage({
            onLeave: function (index, nextIndex, dircetion) {
                $(this).find('.component').trigger("onLeave")
            },
            afterLoad: function (anchorLink, index) {
                $(this).find('.component').trigger("afterLoad")
            }
        }
    )
    this.page[0].find('component').trigger("afterLoad")
    this.el.show()
    if (firstPage) {
        $.fn.fullpage.moveTo(firstPage);
    }
}
///<jscompress sourcefile="ComponentBase.js" />
/**
 * Created by Dovahkiin on 2017/4/2.
 */
var ComponentBase = function (name, cfg) {
    var cfg = cfg || {}
    var id = ('Cpt_' + Math.random()).replace('.', '_')
    var cls = 'Cpt_' + cfg.type
    var component = $('<div class="component ' + cls + ' component_name_' + name + '" id="' + id + '"></div>')
    cfg.text && component.text(cfg.text)
    cfg.width && component.width(cfg.width / 2)
    cfg.height && component.height(cfg.height / 2)
    cfg.css && component.css(cfg.css)
    cfg.bg && component.css('backgroundImage', 'url(' + cfg.bg + ')')
    if (cfg.center === true) {
        component.css({
            marginLeft: (cfg.width / 4 * -1) + 'px',
            left: '50%'
        })
    }
    if (typeof cfg.onclick === 'function') {
        component.on('click', cfg.onclick);
    }
    component.on('afterLoad', function () {
        setTimeout(function () {
            component.addClass(cls + '_load').removeClass(cls + '_leave')
            cfg.animateIn && component.animate(cfg.animateIn)
        }, cfg.delay || 0)
        return false
    })
    component.on('onLeave', function () {
        setTimeout(function () {
            component.addClass(cls + '_leave').removeClass(cls + '_load')
            cfg.animateOut && component.animate(cfg.animateOut)
        }, cfg.delay || 0)
        return false
    })
    return component
}

///<jscompress sourcefile="ComponentBar.js" />
/**
 * Created by Dovahkiin on 2017/4/2.
 */
var ComponentBar = function (name, cfg) {
    var component = new ComponentBase(name, cfg)
    $.each(cfg.data, function (index, item) {
        var line = $('<div class="line"></div>')
        var name = $('<div class="name"></div>')
        var rate = $('<div class="rate"></div>')
        var per = $('<div class="per"></div>')
        var width = item[1] * 100 + '%'
        var bgStyle = ''
        if (item[2]) {
            bgStyle = 'style="background-color:' + item[2] + '"'
        }
        rate.css('width', width)
        rate.html('<div class="bg"' + bgStyle + '></div>')
        name.text(item[0])
        line.append(name).append(rate).append(per)
        per.text(width)
        component.append(line)
    })
    return component
}

///<jscompress sourcefile="ComponentBar_v.js" />
/**
 * Created by Dovahkiin on 2017/4/5.
 */
/* 垂直柱图组件对象 */
var ComponentBar_v = function (name, cfg) {
    //  任务二：(1) 完成 component 的初始化定义（补全 var component = ???）
    var component = new ComponentBar(name, cfg);
    //  任务二：(2) 完成 width 每个柱图中项目的宽度计算。（补全 var width = ???）
    var width = ( 100 / cfg.data.length ) >> 0;
    component.find('.line').width(width + '%');
    $.each(component.find('.rate'), function () {
        var w = $(this).css('width');
        //  任务二：(3) 把进度区的宽度重设为高度，并且取消原来的宽度
        $(this).height(w).width('');
    });
    $.each(component.find('.per'), function () {
        //  任务二：(4) 重新调整 DOM 结构，把百分比数值(.per)添加到 进度区 (.rate)中，和色块元素(.bg)同级。提示，获得 进度区 元素：$(this).prev()
        $(this).appendTo($(this).prev());
    })
    return component;
}
///<jscompress sourcefile="ComponentPie.js" />
/**
 * Created by Dovahkiin on 2017/4/5.
 */
var ComponentPie = function (name, cfg) {
    var component = new ComponentBase(name, cfg)
    var width = cfg.width
    var height = cfg.height
    var cns = document.createElement('canvas')
    var ctx = cns.getContext('2d')
    cns.width = ctx.width = width
    cns.height = ctx.height = height
    $(cns).css('zIndex', 1)
    component.append(cns)
    var r = width / 2
    //底图层
    ctx.beginPath()
    ctx.fillStyle = '##282828'
    ctx.strokeStyle = '##282828'
    ctx.lineWidth = 1
    ctx.arc(r, r, r, 0, 2 * Math.PI)
    ctx.fill()
    ctx.stroke()
    //绘制数据层
    var cns = document.createElement('canvas')
    var ctx = cns.getContext('2d')
    cns.width = ctx.width = width
    cns.height = ctx.height = height
    $(cns).css('zIndex', 2)
    component.append(cns)
    var colors = ['black', 'blue', 'green', 'cyan', 'gery', 'orange']//备用颜色
    var startAngle = 1.5 * Math.PI//开始角度
    var endAngle = 0//结束角度
    var aAngel = Math.PI * 2//100%结束的圆角

    var step = cfg.data.length
    for (var i = 0; i < step; i++) {
        var item = cfg.data[i]
        endAngle = startAngle + aAngel * item[1]
        var color = item[2] || (item[2] = colors.pop())
        ctx.beginPath()
        ctx.fillStyle = color
        ctx.strokeStyle = color
        ctx.lineWidth = 0.1
        ctx.moveTo(r, r)
        ctx.arc(r, r, r, startAngle, endAngle)
        ctx.fill()
        ctx.stroke()
        startAngle = endAngle
        //加入文本与百分百
        var text = $('<div class="text"></div>')//TODO css modify size
        text.text(item[0])
        var per = $('<div class="per"></div>')
        per.text(item[1] * 100 + '%')
        text.append(per)
        var x = r + Math.sin(0.5 * Math.PI - startAngle) * r
        var y = r + Math.cos(0.5 * Math.PI - endAngle) * r
        if (x >= width / 2) {
            text.css('left', x / 2)
        } else {
            text.css('right', (width - x) / 2)
        }
        if (y >= height / 2) {
            text.css('top', y / 2)
        } else {
            text.css('bottom', (height - y) / 2)
        }
        if (item[2]) {
            text.css('color', item[2])
        }
        text.css('transition', 'all .5s ' + i * .2 + 's')//顺序延迟
        text.css('opacity', 0)
        component.append(text)
    }
    //加入蒙版层
    var cns = document.createElement('canvas')
    var ctx = cns.getContext('2d')
    cns.width = ctx.width = width
    cns.height = ctx.height = height
    $(cns).css('zIndex', 3)
    component.append(cns)

    ctx.fillStyle = '#2c2c2c'
    ctx.strokeStyle = '#2c2c2c'
    ctx.lineWidth = 1


    //生长动画
    startAngle = 1.5 * Math.PI
    var draw = function (per) {
        ctx.clearRect(0, 0, width, height)
        ctx.beginPath()
        ctx.moveTo(r, r)
        if (per <= 0) {
            ctx.arc(r, r, r, 0, aAngel)
            component.find('.text').css('opacity', 0)
        }
        else {
            ctx.arc(r, r, r, startAngle, startAngle + aAngel * per, true)
        }
        ctx.fill()
        ctx.stroke()
        if (per >= 1) {
            component.find('.text').css('transition', 'all 0s')
            ComponentPie.reSort(component.find('.text'))
            component.find('.text').css('transition', 'all 1s')
            component.find('.text').css('opacity', 1)
        }
    }

    draw(0)

    component.on('afterLoad', function () {
        var s = 0
        for (var i = 0; i < 100; i++) {
            setTimeout(function () {
                s += 0.01
                draw(s)
            }, i * 10)
        }
    })
    component.on('onLeave', function () {
        var s = 1
        for (var i = 0; i < 100; i++) {
            setTimeout(function () {
                s -= 0.01
                draw(s)
            }, i * 10)
        }
    })
    return component
}
ComponentPie.reSort = function (list) {
    //检测相交
    var compare = function (domA, domB) {
        var offsetA = $(domA).offset//直接获得偏移值
        var offsetB = $(domB).offset
        var shadowA_x = [offsetA.left, $(domA).width() + offsetA.left]
        var shadowA_y = [offsetA.top, $(domA).height() + offsetA.top]
        var shadowB_x = [offsetB.left, $(domB).width() + offsetB.left]
        var shadowB_y = [offsetB.top, $(domB).height() + offsetB.top]
        var intersect_x = (shadowA_x[0] > shadowB_x[0] && shadowA_x[0] < shadowB_x[1]) || (shadowA_x[1] > shadowB_x[0] && shadowA_x[1] < shadowB_x[1])
        var intersect_y = (shadowA_y[0] > shadowB_y[0] && shadowA_y[0] < shadowB_y[1]) || (shadowA_y[1] > shadowB_y[0] && shadowA_y[1] < shadowB_y[1])
        return intersect_x && intersect_y
    }
    //错开重排
    var reset = function (domA, domB) {
        if ($(domA).css('top') != 'auto') {
            $(domA).css('top', parseInt($(domA).css('top')) + $(domB).height())
        }
        if ($(domA).css('bottom') != 'auto') {
            $(domA).css('bottom', parseInt($(domA).css('bottom')) + $(domB).height())
        }
    }
    var willReset = [list[0]]
    $.each(list, function (i, domTarget) {
        if (compare(willReset[willReset.length - 1], domTarget)) {
            willReset.push(domTarget)
        }
        if (willReset.length > 1) {
            $.each(willReset, function (i, domA) {
                if (willReset[i + 1])
                    reset(domA, willReset[i + 1])
            })
            ComponentPie.reSort(willReset)
        }
    })
}
///<jscompress sourcefile="ComponentRing.js" />
/**
 * Created by Dovahkiin on 2017/4/5.
 */
/* 环图组件对象 */

var ComponentRing = function (name, cfg) {
    if (cfg.data.length > 1) {  //  环图应该只有一个数据
        // 任务二：(1) 把数据格式化为只有一项，例如 a = [ [1] , [2] , [3] ] 格式化为： a=[ [1] ]
        cfg.data = [cfg.data[0]];
    }
    //  任务二：(2) 重设配置中的 type 参数，不仅利用 H5ComponentPie 构建 DOM 结构和 JS 逻辑，也使用其 CSS 样式定义（思考下为什么能达到这个效果）
    cfg.type = 'pie';
    var component = new ComponentPie(name, cfg);
    //  任务二：(3) 修正组件的样式，以支持在样式文件中组件的样式定义 .h5_component_ring 相关样式能生效
    component.addClass('Cpt_ring');
    var mask = $('<div class="mask">');
    // 任务二：(4) 把创建好的遮罩元素添加到组件中
    component.append(mask);
    var text = component.find('.text');
    text.attr('style', '');
    if (cfg.data[0][2]) {
        text.css('color', cfg.data[0][2]);
    }
    mask.append(text);
    return component;
}
///<jscompress sourcefile="ComponentPoint.js" />
/**
 * Created by Dovahkiin on 2017/4/2.
 */
var ComponentPoint = function (name, cfg) {
    var component = new ComponentBase(name, cfg)
    var base = cfg.data[0][1]
    $.each(cfg.data, function (index, item) {
        var point = $('<div class="point point_' + index + '"></div>')
        var name = $('<div class="name">' + item[0] + '</div>')
        var rate = $('<div class="per">' + (item[1] * 100) + '%</div>')
        var per = (item[1] / base * 100) + '%'
        name.append(rate)
        point.append(name)
        point.width(per).height(per)
        if (item[2]) {
            point.css('background-color', item[2])
        }
        if (item[3] !== undefined && item[4] !== undefined) {
            point.css('left', item[3]).css('top', item[4])
        }
        component.append(point)
    })
    function Breath() {
        component.find('.point').removeClass('point_focus');
        $(this).addClass('point_focus');
        return false;
    }

    component.find('.point').on('click', function () {
        component.find('.point').removeClass('point_focus');
        $(this).addClass('point_focus');
        return false;
    })
    component.find('.point').on('mouseover', function () {
        component.find('.point').removeClass('point_focus');
        $(this).addClass('point_focus');
        return false;
    }).eq(0).addClass('point_focus')
    return component
}

///<jscompress sourcefile="ComponentPolyline.js" />
/**
 * Created by Dovahkiin on 2017/4/5.
 */
var ComponentPolyline = function (name, cfg) {
    var component = new ComponentBase(name, cfg)
    var width = cfg.width
    var height = cfg.height
    var cns = document.createElement('canvas')
    var ctx = cns.getContext('2d')
    cns.width = ctx.width = width
    cns.height = ctx.height = height
    component.append(cns)
    var step = 10
    ctx.beginPath()
    ctx.lineWidth = 1
    ctx.strokeStyle = 'white'
    window.ctx = ctx
    //画网格线
    for (var i = 0; i < step + 1; i++) {
        var y = height / step * i
        ctx.moveTo(0, y)
        ctx.lineTo(width, y)
    }
    step = cfg.data.length + 1
    var text_w = width / step >> 0
    for (var i = 0; i < step + 1; i++) {
        var x = (width / step) * i
        ctx.moveTo(x, 0)
        ctx.lineTo(x, height)
        if (cfg.data[i]) {
            var text = $('<div class="text"></div>')
            text.text(cfg.data[i][0])
            text.css('width', text_w / 2).css('left', x / 2 + text_w / 4)
            component.append(text)
        }
    }
    ctx.stroke()
    //画折线
    var cns = document.createElement('canvas')
    var ctx = cns.getContext('2d')
    cns.width = ctx.width = width
    cns.height = ctx.height = height
    component.append(cns)
    var draw = function (per) {
        ctx.clearRect(0, 0, width, height)
        ctx.beginPath()
        ctx.lineWidth = 3
        ctx.strokeStyle = '#c5c2c2'
        //描点
        var x = 0
        var y = 0
        var pst_x = (width / step) + 1
        for (i in cfg.data) {
            var item = cfg.data[i]
            x = pst_x * i + pst_x
            y = height - item[1] * height * per
            ctx.moveTo(x, y)
            ctx.arc(x, y, 5, 0, 2 * Math.PI)
        }
        //连线
        ctx.moveTo(0, height)
        ctx.lineTo(pst_x, height - cfg.data[0][1] * height * per)
        ctx.moveTo(pst_x, height - cfg.data[0][1] * height * per)
        for (i in cfg.data) {
            var item = cfg.data[i]
            x = pst_x * i + pst_x
            y = height - item[1] * height * per
            ctx.lineTo(x, y)
        }
        ctx.lineTo(width, height)
        ctx.stroke()
        //绘制阴影
        ctx.lineWidth = 1
        ctx.strokeStyle = 'rgba(0,0,0,0)'
        ctx.fillStyle = 'rgba(197,194,194,0.2)'
        //写数据
        ctx.lineTo(x, height)
        ctx.lineTo(0, height)
        ctx.fill()
        for (i in cfg.data) {
            var item = cfg.data[i]
            x = pst_x * i + pst_x
            y = height - item[1] * height * per
            ctx.moveTo(x, y)
            ctx.fillStyle = item[2] ? item[2] : '#595959'
            ctx.font="35px Arial"
            ctx.fillText(((item[1] * 100) >> 0) + '%', x - 10, y - 10)
        }
        ctx.stroke()
    }
    component.on('afterLoad', function () {
        var s = 0
        for (var i = 0; i < 100; i++) {
            setTimeout(function () {
                s += 0.01
                draw(s)
            }, i * 10 + 500)
        }
    })
    component.on('onLeave', function () {
        var s = 1
        for (var i = 0; i < 100; i++) {
            setTimeout(function () {
                s -= 0.01
                draw(s)
            }, i * 10 + 500)
        }
    })
    return component
}

///<jscompress sourcefile="ComponentRadar.js" />
/**
 * Created by Dovahkiin on 2017/4/5.
 */
var ComponentRadar = function (name, cfg) {
    var component = new ComponentBase(name, cfg)
    var width = cfg.width
    var height = cfg.height
    var cns = document.createElement('canvas')
    var ctx = cns.getContext('2d')
    cns.width = ctx.width = width
    cns.height = ctx.height = height
    component.append(cns)
    var r = width / 2
    // ctx.beginPath()
    // ctx.arc(r, r, 5, 0, 2 * Math.PI)
    // ctx.stroke()
    // ctx.beginPath()
    // ctx.arc(r, r, r, 0, 2 * Math.PI)
    // ctx.stroke()
    //计算多边形顶点坐标
    //圆心(a,b)
    //rad=(2*Math.PI/360)*(360/data.length)*i
    //x=a+Math.sin(rad)*r
    //y=b+Math.cos(rad)*r
    var step = cfg.data.length
    var isBlue = false
    for (var s = 10; s > 0; s--) {
        ctx.beginPath()
        for (var i = 0; i < step; i++) {
            var rad = (2 * Math.PI / 360) * (360 / step) * i
            var x = r + Math.sin(rad) * r * (s / 10)
            var y = r + Math.cos(rad) * r * (s / 10)
            ctx.lineTo(x, y)
        }
        ctx.closePath()
        ctx.fillStyle = (isBlue = !isBlue) ? '#696868' : '#565858'
        ctx.fill()
        // ctx.stroke()
    }
    //绘制伞骨
    for (var i = 0; i < step; i++) {
        var rad = (2 * Math.PI / 360) * (360 / step) * i
        var x = r + Math.sin(rad) * r
        var y = r + Math.cos(rad) * r
        ctx.moveTo(r, r)
        ctx.lineTo(x, y)
        //输出项目名
        if (cfg.data[i]) {
            var text = $('<div class="text"></div>')
            text.text(cfg.data[i][0])
            text.css('transition', 'all .5s ' + i * .2 + 's')//顺序延迟
            if (x > width / 2) {
                text.css('left', x / 2 + 5)
            }
            else {
                text.css('right', (width - x) / 2 + 5)
            }
            if (y > height / 2) {
                text.css('top', y / 2 + 5)
            } else {
                text.css('bottom', (height - y) / 2 + 5)
            }
            if (cfg.data[i][2]) {
                text.css('color', cfg.data[i][2])
            }
            text.css('opacity', 0)
            // text.css('width', text_w / 2).css('left', x / 2 + text_w / 4)
            component.append(text)
        }
    }
    ctx.lineWidth=5
    ctx.strokeStyle = '#e0e0e0'
    ctx.stroke()
    ctx.closePath()
    var cns = document.createElement('canvas')
    var ctx = cns.getContext('2d')
    cns.width = ctx.width = width
    cns.height = ctx.height = height
    component.append(cns)
    ctx.strokeStyle = '#e87777'
    var draw = function (per) {
        if (per >= 1) {
            component.find('.text').css('opacity', 1)
        }
        if (per <= 1) {
            component.find('.text').css('opacity', 0)
        }
        //输出数据折线
        ctx.clearRect(0, 0, width, height)
        ctx.beginPath()
        ctx.lineWidth=20
        for (var i = 0; i < step; i++) {
            var rad = (2 * Math.PI / 360) * (360 / step) * i
            var rate = cfg.data[i][1] * per
            var x = r + Math.sin(rad) * rate * r
            var y = r + Math.cos(rad) * rate * r
            ctx.lineTo(x, y)
        }
        ctx.closePath()
        ctx.stroke()
        ctx.fillStyle = '#e87777'
        for (var i = 0; i < step; i++) {
            var rad = (2 * Math.PI / 360) * (360 / step) * i
            var rate = cfg.data[i][1]
            var x = r + Math.sin(rad) * rate * per * r
            var y = r + Math.cos(rad) * rate * per * r
            ctx.beginPath()
            ctx.arc(x, y, 5, 0, 2 * Math.PI)
            ctx.fill()
            ctx.closePath()
        }
    }
    component.on('afterLoad', function () {
        var s = 0
        for (var i = 0; i < 100; i++) {
            setTimeout(function () {
                s += 0.01
                draw(s)
            }, i * 10 + 500)
        }
    })
    component.on('onLeave', function () {
        var s = 1
        for (var i = 0; i < 100; i++) {
            setTimeout(function () {
                s -= 0.01
                draw(s)
            }, i * 10 + 500)
        }
    })
    return component
}
