var oUp = document.getElementsByClassName("icon-shangyishou")[0]
var oDo = document.getElementsByClassName("icon-xiayishou")[0]
var oTotal = document.getElementsByClassName("total_time")[0]
var oAudio = document.getElementById("audio")
var oPlay = document.getElementById("play")
var oCircle = document.getElementById("circle")
var oStart = document.getElementById("start_time")
var oRound = document.getElementById("round")
var oLoad = document.getElementById("load")
var oList = document.getElementsByClassName("music_list")[0]
var oUl = oList.getElementsByTagName("ul")[0]

var _this = null
var num = 0//数组中计数的工具
var arr = ["music/10.mp3", "music/9.mp3", "music/8.mp3", "music/7.mp3", "music/6.mp3", "music/5.mp3", "music/4.mp3", "music/3.mp3", "music/2.mp3", "music/1.mp3"]
var arr2 = ["ツギハギスタッカート","aLIEz","&Z","heavenly blue","明月天涯","Valder Fields","我是全英雄联盟最骚的骚猪","茜さす", "Shape of You", "Three Magic"]
var arr3 = ["9.7MB","14.3MB","7.7MB","10.2MB","11.4MB","6.2MB","8.2MB","3.8MB", "6.7MB", "9.0MB"]
oAudio.src = arr[num]//链接到歌曲文件
for (var i = 0; i < arr.length; i++) {//显示歌曲信息
    var str = '<li index=' + i + '>' + '<span>' + arr2[i] + '</span>' + arr3[i] + '</li>'
    oUl.innerHTML += str

}
var oLi = oUl.getElementsByTagName("li")

//转换总时长
oAudio.addEventListener("canplay", function () {
    oTotal.innerHTML = getMin(this.duration)//this.duration-获得当前视频的长度：
})

//播放列表
for (var i = 0; i < oLi.length; i++) {
    oLi[i].ondblclick = function () {
        oAudio.src = arr[this.getAttribute("index")]//获得链接的index属性
        _this = this//当前曲目
        oPlay.innerHTML = '<button class="iconfont icon-zanting"><img src="img/暂停.png" width="20" height="20"/></button>'
    }

}

//暂停键和播放键
oPlay.onclick = function () {
    if (oAudio.paused) {
        oAudio.play()
        oPlay.innerHTML = '<button class="iconfont icon-zanting"><img src="img/暂停.png" width="20" height="20"/></button>'

    } else {
        oAudio.pause()
        oPlay.innerHTML = '<button class="iconfont icon-bofang"><img src="img/播放.png" width="20" height="20"/></button>'
    }
}

//监听进度变化
oAudio.ontimeupdate = function () {//歌曲时长转换成进度条200px
    var pre = Math.floor(oAudio.currentTime / oAudio.duration * 200)//currentTime-音频播放的时间 歌曲时长转换成进度条200px
    oCircle.style.width = pre + "px"
    oStart.innerHTML = getMin(oAudio.currentTime)
    oRound.style.left = oCircle.style.width
}

//点击进度变化
oLoad.onclick = function (e) {
    var l = e.offsetX - oLoad.offsetLeft
    var n = l + 100
    oAudio.currentTime = (n / 200) * oAudio.duration
    if (oAudio.paused) {
        oPlay.innerHTML = '<button class="iconfont icon-zanting"><img src="img/播放.png" width="20" height="20"/></button>'
    } else {
        oPlay.innerHTML = '<button class="iconfont icon-bofang"><img src="img/暂停.png" width="20" height="20"/></button>'
    }

}
//拖拽原点
oRound.onmousedown = function (e) {
    document.onmousemove = function (e) {
        var l = e.offsetX - oLoad.offsetLeft
        var n = l + 100
        oAudio.currentTime = (n / 200) * oAudio.duration

    }
    document.onmouseup = function () {
        document.onmousedown = null
        document.onmousemove = null
        //清除鼠标按下起来后的事件执行
    }
    return false

}

//播放结束
oAudio.addEventListener("ended", function () {
    num++
    oAudio.src = arr[num]
    if (num >=arr.length) {
        num = 0
    }
    console.log(oAudio.src)
    oAudio.play()
})

//获取分钟的函数
function getMin(time) {
    var m = Math.floor(time / 60)//返回小于或等于一个给定数字的最大整数
    var s = Math.floor(time % 60)
    if (m <= 9) {
        m = "0" + m
    }
    if (s <= 9) {
        s = "0" + s
    }
    return m + ":" + s
}

_this = oLi[0]

//上一首
oUp.onclick = function () {
    var now = _this.getAttribute("index")
    now--
    if (now < 0) {
        now = arr.length - 1
    }
    console.log(now)
    _this = oLi[now]
    oAudio.src = arr[now]
    oAudio.play()
    console.log(oAudio.src)
    oPlay.innerHTML = '<button class="iconfont icon-zanting"><img src="img/暂停.png" width="20" height="20"/></button>'
}
//下一首
oDo.onclick = function () {
    var now = _this.getAttribute("index")
    now++
    if (now > arr.length - 1) {
        now = 0
    }
    _this = oLi[now]
    console.log(now)
    oAudio.src = arr[now]
    oAudio.play()
    console.log(oAudio.src)
    oPlay.innerHTML = '<button class="iconfont icon-zanting"><img src="img/暂停.png" width="20" height="20"/></button>'
}