// pages/saveimg/saveimg.js
Page({

  /**
   * 页面的初始数据
   */
  data: {
  
  },

  getAuthSave() {
    var that = this
    // 获取授权
    wx.getSetting({
      success(res) {
        console.log(res.authSetting)
        if (!res.authSetting['scope.writePhotosAlbum']) {
          console.log('in')
          
          wx.authorize({
            scope: 'scope.writePhotosAlbum',
            success() {
              console.log('授权成功')
              that.save()
            },
            fail() {
              console.log('授权失败')
              console.log(res.authSetting['scope.writePhotosAlbum'])
              if (res.authSetting['scope.writePhotosAlbum'] === false) {
                wx.openSetting({
                  success: (res) => {
                    console.log('openSetting success')
                    console.log(res.authSetting)
                  },
                  fail(res) {
                    console.log('openSetting fail')
                    console.log(res.authSetting)
                  }
                })
              }

              
            }
          })
        } else {
          that.save()
        }
      }
    })

    

  },

  save() {
    //文件下载
    var imgSrc = "http://cdn.meimaonong.com/uploads/imgs3/9.jpg"
    wx.showLoading({
      title: '',
      mask: true,
    })
    wx.downloadFile({
      url: imgSrc,
      success:
      function (res) {
        console.log(res);
        //图片保存到本地
        wx.saveImageToPhotosAlbum({
          filePath: res.tempFilePath,
          success:
          function (data) {
            wx.hideLoading()
            console.log(data);
          }
          
        })
      }
    })
  },

  /**
   * 生命周期函数--监听页面加载
   */
  onLoad: function (options) {
  
  },

  /**
   * 生命周期函数--监听页面初次渲染完成
   */
  onReady: function () {
  
  },

  /**
   * 生命周期函数--监听页面显示
   */
  onShow: function () {
  
  },

  /**
   * 生命周期函数--监听页面隐藏
   */
  onHide: function () {
  
  },

  /**
   * 生命周期函数--监听页面卸载
   */
  onUnload: function () {
  
  },

  /**
   * 页面相关事件处理函数--监听用户下拉动作
   */
  onPullDownRefresh: function () {
  
  },

  /**
   * 页面上拉触底事件的处理函数
   */
  onReachBottom: function () {
  
  },

  /**
   * 用户点击右上角分享
   */
  onShareAppMessage: function () {
  
  }
})