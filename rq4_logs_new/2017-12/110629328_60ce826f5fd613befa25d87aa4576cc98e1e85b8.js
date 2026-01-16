
/**
 *---------------------------------------------------------
 * Fn_name: date
 * Fn_DES: 格式化日期
 * @param:  date,/type: 日期的原始值/ 默认值: / 描述: 
 * @param:  format,/type: string/ 默认值: 'yyyy-MM-dd'/ 描述: 
 *        y:表示年，四位'yyyy'和两位'yy'
 *        M:表示月，'MM' 'M'
 *        d:表示日，同上
 *        h:表示小时；m:表示分；s:表示秒
 * 
 *---------------------------------------------------------
 */

export const date = function (date, format) {
  // debugger
    if (!date) return;
    var fmt = format ? format : 'yyyy-MM-dd';
    var date = new Date(Number(date));
    var time = {
      y: date.getFullYear(),
      M: date.getMonth() + 1,
      d: date.getDate(),
      h: date.getHours(),
      m: date.getMinutes(),
      s: date.getSeconds()
    }
    //格式化：今天/昨天/更久
    fmt = fmt.replace(/C/g, function (v) {
      var c = '更久';
      if ((v.length > 0) && (time.y == new Date().getFullYear()) && (time.M == new Date().getMonth() + 1)) {
        if (time.d == new Date().getDate()) {
          c = '今天'
        } else if (time.d == new Date().getDate() - 1) {
          c = '昨天'
        }
      }
      return c;
    })
    // 格式化
    fmt = fmt.replace(/M+|d+|h+|m+|s+/g, function (v) {
      return ((v.length > 1 ? '0' : '') + time[v.slice(-1)]).slice(-2);
    });
    // 格式化年
    fmt = fmt.replace(/y+/g, function (v) {
      return time.y.toString().slice(-(v.length))
    });
    return fmt
  }

  
  /**
   *---------------------------------------------------------
   * @name: 
   * @description: 
   * @param:  ,/type: / 默认值: / 描述: 
   * @example: 
   * 
   *---------------------------------------------------------
   */
  
   export const fmtTime = function(ms){
      let m = (Math.round(ms/1000))%3600%60;
      let s = Math.ceil((Math.round(ms/1000)-m)%3600/60);
      let h = Math.ceil((Math.round(ms/1000)-m)/3600);
      [m,s,h].forEach((v)=>{
        (String(v).length > 1 ? '0' : '') + v
      })
      return `${h}:${s}:${m}`   
   }