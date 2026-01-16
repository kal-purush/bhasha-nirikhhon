// 画带箭头的线条
Raphael.fn.arrowLine = function(line) {
  var r = this;

  // 箭头角度 长度
  var AR = 30;
  var AL = 10;

  var rad = Math.PI / 180;

  r.path(line).attr({
    stroke: "#a26fdb",
    "stroke-width": 1
  });

  line = line.replace("M", "");
  line = line.replace("L", "");
  line = line.split(",");
  console.log(line);
  var x1 = parseInt(line[0]),
    y1 = parseInt(line[1]),
    x2 = parseInt(line[2]),
    y2 = parseInt(line[3]);

  var x = x1 - x2,
    y = y1 - y2;
  var x_ = Math.abs(x),
    y_ = Math.abs(y);
  var anger = Math.atan(x_ / y_) / rad;

  var AO = {};
  if (x < 0 && y > 0) {
    anger = anger;
  } else if (x < 0 && y < 0) {
    anger = 180 - anger;
  } else if (x > 0 && y < 0) {
    anger = 180 + anger;
  } else if (x > 0 && y > 0) {
    anger = 360 - anger;
  }
  AO = {
    a1: "M" + x1 + "," + y1 + ",L" + (x1 + AL * Math.sin((anger - AR) * rad)) + "," + (y1 + AL * Math.cos((180 + anger - AR) * rad)),
    a2: "M" + x1 + "," + y1 + ",L" + (x1 + AL * Math.sin((anger + AR) * rad)) + "," + (y1 + AL * Math.cos((180 + anger + AR) * rad))
  }


  r.path(AO.a1).attr({
    stroke: "#a26fdb",
    "stroke-width": 1
  });
  r.path(AO.a2).attr({
    stroke: "#a26fdb",
    "stroke-width": 1
  });

}

$(function() {
  Raphael("container2", 400, 400, function() {
    var r = this;
    // 绘制地图
    r.setStart();

    var rect = r.rect(0, 0, 400, 400).attr({
      fill: "#ffffff",
      stroke: "#000",
      "stroke-width": 1
    });

    var oneLine = "M220,200,L290,10";
    var twoLine = "M210,210,L250,350";
    var threeLine = "M190,200,L80,290";
    var fourLine = "M200,190,L90,10";

    // 入口
    r.arrowLine(oneLine);
    r.arrowLine(twoLine);
    r.arrowLine(threeLine);
    r.arrowLine(fourLine);
  });
});