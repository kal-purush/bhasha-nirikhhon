import Vue from 'vue'
import axios from 'axios'

// 设置基准路径
axios.defaults.baseURL = 'https://www.zhengzhicheng.cn/api/public/v1/'

// 可以替换带axios底层发送请求的实现
axios.defaults.adapter = function(config) {
  return new Promise((resolve, reject) => {
    // console.log(config)
    wx.showLoading({
      title: '数据正在加载中...', //提示的内容
      mask: true //显示透明蒙层，防止触摸穿透
    })
    wx.request({
      url: config.url, //开发者服务器接口地址",
      data: config.data, //请求的参数",
      method: config.method.toUpperCase(),
      // TODO 今天不配置header，后面还要设置
      success: res => {
        resolve(res)
      },
      fail: err => {
        reject(err)
      },
      complete: () => {
        // 无论成功还是失败，都会执行到这里
        wx.hideLoading()
      }
    })
  })
}

Vue.prototype.$axios = axios