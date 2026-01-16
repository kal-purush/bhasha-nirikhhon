export default {
  pages: [
    'pages/index/index', // 首页 信息页
    'pages/money/index', // 资产信息页
    'pages/houseList/index', // 房屋列表页
    'pages/houseDetail/index', // 房屋详情页
    'pages/map/index', // 地图分布页
    'pages/contractList/index', // 合同列表页
    'pages/contractDetail/index', // 合同详情页
    'pages/customerList/index', // 客户列表页
    'pages/customerDetail/index', // 客户详情页
    'pages/toDoList/index', // 代办列表页
    'pages/toDoDetail/index', // 代办详情页
    'pages/chartList/index', // 图表页
    'pages/chartDetail/index', // 图表详情页
    'pages/my/index', // 我的页
    'pages/login/index', // 登录页 
    'pages/about/index', // 关于页
    'pages/quit/index', // 退出页
  ],
  window: {
    backgroundTextStyle: 'light',
    navigationBarBackgroundColor: '#fff',
    navigationBarTitleText: 'WeChat',
    navigationBarTextStyle: 'black'
  },
  tabBar: {
    custom: true,
    color: 'rgba(68, 68, 68, 1)',
    selectedColor: 'rgba(68, 68, 68, 1)',
    backgroundColor: 'white',
    list: [
      {
        pagePath: 'pages/index/index',
        text: '信息',
        iconPath: 'images/icon1.png',
        selectedIconPath: 'images/icon1-active.png'
      },
      {
        pagePath: 'pages/toDoList/index',
        text: '代办',
        iconPath: 'images/icon2.png',
        selectedIconPath: 'images/icon2-active.png'
      },
      {
        pagePath: 'pages/chartList/index',
        text: '图表',
        iconPath: 'images/icon3.png',
        selectedIconPath: 'images/icon3-active.png'
      },
      {
        pagePath: 'pages/my/index',
        text: '我的',
        iconPath: 'images/icon4.png',
        selectedIconPath: 'images/icon4-active.png'
      }]
  }
}