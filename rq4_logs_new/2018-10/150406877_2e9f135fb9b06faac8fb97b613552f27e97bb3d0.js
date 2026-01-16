import React from 'react'
import moment from 'moment';
import 'moment/locale/zh-cn';
import "./imgcalendar.less";
moment.locale('zh-cn');
/**
 * 自定义日历组建
 * 显示2周的图片信息
 * 组建包含事件：1.初始化
 *              2.鼠标悬停
 *              3.鼠标点击
 *              4.翻到前一周
 */
class ImgCalendar extends React.Component {
  state={
    ArrayData:[]
  }
  constructor(props){
    super( props )
    this.state={};
  }
  getimgdate = ()=>{
    //获取之前14天的记录
    //初始时间到目标时间的时间数组
    let ArrayData = [];
    for(var i=1;i<15;i++){
      ArrayData.push(moment().subtract('days',i).date());
    }
    return (
      <ul className="events">
        {
          ArrayData.map(item => (
            <li key={item}>
            {item}
            </li>
          ))
        }
      </ul>
    );
  }

  render() {
    return(
    <div>
    {
      this.getimgdate()
    }</div>
    );
  }
}

export default ImgCalendar;
