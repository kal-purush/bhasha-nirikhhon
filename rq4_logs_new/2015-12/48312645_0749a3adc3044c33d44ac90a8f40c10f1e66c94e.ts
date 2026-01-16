import {Component} from 'angular2/core';
import {RouteConfig, ROUTER_DIRECTIVES} from 'angular2/router';
// import {Hero} from './hero';
import {HomeComponent} from './home.component';



@Component({
  selector: 'my-app',
  template: `
      <div id="header">
      <h1>鳄鱼的日语教室</h1>
      <div class="apply">
        <img class="title-pig" src="./images/title-pig.png">
        <img class="title-shine" src="./images/title-shine.png">
      </div>
      <img class="title-girl" src="./images/title-girl.png">
      <div class="board">
        
      </div>
    </div>
    <div id="menu-wrap">
      <ul id="menu">
        <li>首页<img src="./images/menu-1.png"></li>
        <li>视频<img src="./images/menu-2.png"></li>
        <li>课程<img src="./images/menu-3.png"></li>
        <li>留学<img src="./images/menu-4.png"></li>
      </ul>
    </div>
    <router-outlet></router-outlet>
  `,
  styles:[`
    #header {
      position: relative;  
    }
    #header h1 {
      width: 718px;
      height: 118px;
      margin: 42px auto 16px;
      text-indent: -999px;
      background: url('./images/title.png') no-repeat;
      background-size: contain;
    }
    /* 右侧块 */
    #header .board {
      width: 106px;
      height: 126px;
      position: absolute;
      top: -40px;
      right: 18px;
      background: url('./images/board.png') no-repeat;
      background-size: contain;
    }
    #header .title-girl {
      width: 90px;
      height: 80px;
      position: absolute;
      top: 32px;
      right: 196px;
    }
    /* 左侧块 */
    #header .apply {
      width: 254px;
      height: 156px;
      position: absolute;
      top: 0;
      left: 0;
    }
    #header .apply .title-shine {
      width: 196px;
      height: 86px;
      position: absolute;
      top: 0;
      right: 0;
      z-index: -1;
    }
    #header .apply .title-pig {
      width: 80px;
      height: 114px;
      position: absolute;
      top: 20px;
      right: 50px;
    }
    /* 菜单 */
    #menu-wrap {
      text-align: center;
    }
    #menu {
      display: inline-block;
      height: 66px;
      margin: 0 auto;
    }
    #menu li {
      float: left;
      width: 132px;
      height: 65px;
      margin-right: 38px;
      position: relative;
      line-height: 65px;
      text-align: center;
      background: url('./images/menu.png');
    }
    #menu li img {
      width: 38px;
      height: 38px;
      position: absolute;
      right: -20px;
      bottom: 0;
    }
  `],
  directives: [ROUTER_DIRECTIVES]
})

@RouteConfig([
  {
     path: '/',
     name: 'Home',
     component: HomeComponent,
     useAsDefault: true
  }
])

export class AppComponent {

}