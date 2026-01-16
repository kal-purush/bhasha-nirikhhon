var 公共函数 = {};
 
公共函数.加法 = function (a,b) {
  return a + b;
}
 
公共函数.日期年月日 = function () {
  var 时间 = new Date();
  var 年 = 时间.getFullYear(), 月 = 时间.getMonth() + 1, 日 = 时间.getDate();
  var 当前时间 = 年 + "-" + 月 + "-" + 日;
  return 当前时间;
}
 
公共函数.获取外网IP和归属地 = function () {
  var getIp_api = http.get('http://pv.sohu.com/cityjson?ie=utf-8');
  var InetIP = getIp_api.body.string();
  eval(InetIP);
  return returnCitySN.cip + '|' + returnCitySN.cname
};
 
 
module.exports = 公共函数;