var currentPos = 55;
var minCount = 50;
var Count = 10;

var rele = document.getElementById('rele');
var line = document.getElementById('line');

var rightEdge = line.offsetWidth - rele.offsetWidth;
rele.style.left = ((currentPos - minCount) * rightEdge) / Count + "px";
document.getElementById('count').innerHTML = currentPos;

rele.ontouchstart = function (e) {
    var shiftX = e.targetTouches[0].pageX - rele.offsetLeft;
    
    document.ontouchmove = function (e) {
        var newLeft = e.targetTouches[0].pageX - shiftX - line.offsetLeft;
        if (newLeft < 0) { newLeft = 0; }
        if (newLeft > rightEdge) {
            newLeft = rightEdge;
        }
        /*Отобразим значения*/
        document.getElementById('count').innerHTML = Math.round((newLeft / rightEdge) * Count) + minCount;
        rele.style.left = newLeft + 'px';
    }
    document.ontouchend = function () {
        document.ontouchmove = document.ontouchend = null;
    };
    return false;
};