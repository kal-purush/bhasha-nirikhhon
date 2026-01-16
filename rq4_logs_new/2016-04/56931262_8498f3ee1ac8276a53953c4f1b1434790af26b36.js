var aqiData = [
  ["北京", 90],
  ["上海", 50],
  ["福州", 10],
  ["贵阳", 83],
  ["哈尔滨", 65],
  ["乌鲁木齐", 77],
  ["广州", 50],
  ["成都", 90],
  ["西安", 100]
];

(function () {
  /*
  在注释下方编写代码
  遍历读取aqiData中各个城市的数据
  将空气质量指数大于60的城市显示到aqi-list的列表中
  */
  // 获取污染列表
  var list = document.getElementById("aqi-list");
  // 污染城市列表
  var polluCity = [];
  // 污染值存储
  var polluNum  = [];

  function findPolluCity() {
    // 遍历所有城市查找污染城市
    for (i=0; i<aqiData.length; i++) {

      // 获取单个城市信息
      var city = aqiData[i];
      // 获取城市名称
      var cityName = city[0];
      // 获取空气质量指数
      var airQuality = city[1];

      // 判断城市空气质量
      if ( airQuality > 60 ) {
        // 添加污染城市名单/空气质量指数
        polluCity.push(city);
        polluNum.push(airQuality);
      }
    }
  }
  findPolluCity();

  // 对污染指数进行排序 (大-->小)
  function sortNumber (a,b) {
    return b-a;
  }
  var polluList = polluNum.sort(sortNumber);

  function showPolluCity() {
    // 先遍历排序的污染指数
    for (j=0; j<polluList.length; j++) {

      // 假如污染指数相同，避免重新添加
      var cnt = 0;

      // 遍历污染城市进行匹配
      for (i=0; i<polluCity.length; i++) {
        var city = polluCity[i];
        var cityName = city[0];
        var airQuality = city[1];

        // 添加污染项
        if ( airQuality == polluList[j] ) {
          if ( j > 0 && polluList[j] == polluList[j-1] ) {
            // 不执行任何内容
          } else {
            list.innerHTML += "<li><div>"+"第"+(j+1+cnt)+"名:&nbsp;</div><div>&nbsp;"+cityName+" ==> "+airQuality+"</div></li>";
          }
          cnt++;
        }
      }
    }
  }
  showPolluCity();
})();