const { getPost } = require('../../apis/post')
const wemark = require('../../plugins/wemark/wemark')
Page({
  data: {
    wemark: {},
    heightStyle: ''
  },
  onLoad(options) {
    this.setData({
      heightStyle: `height: ${wx.getSystemInfoSync().windowHeight - 16}px;`
    })
    getPost({
      id: options.id,
    }, res => {
      wemark.parse(res.data.content, this, {
        imageWidth: wx.getSystemInfoSync().windowWidth - 40,
        name: 'wemark'
      })
    })
  }
})