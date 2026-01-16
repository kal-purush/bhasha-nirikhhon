//logs.js
var app = getApp()
Page({
  data: {
    cash: '',
    txtArray: [],
    flagId: '',
    money: 0,
    unitPrice : 0,
    tryMe: true,
    coachInfo: '1. 每节课平均60分钟；\n2. 每购买一节课程赠送80分钟健身时长；\n3. 体验课程费用为押金，课程结束后会转为自助健身时长充入您的账户余额中；\n4. 课程购买后，不可退款；\n5. 三次无故旷课，按照一节课计费；'
  },


  changeColor: function (res) {
    var txtArray = this.data.txtArray;
    var tryMe = true;
    for (var i = 0; i < this.data.txtArray.length; i++) {
      console.log(res.currentTarget.id);
      console.log(this.data.txtArray[i].id);
      console.log(res.currentTarget.id == this.data.txtArray[i].id);
      if (res.currentTarget.id == this.data.txtArray[i].id) {
        txtArray[i].changeColor = true;
        if (txtArray[i].num == 1) {
          tryMe = false
        } else {
          tryMe = true
        }
        this.setData({
          flagId: i + 1,
          money: txtArray[i].totalPrice,
          unitPrice: txtArray[i].totalPrice / txtArray[i].num,
          tryMe: tryMe
        })
        console.log("ture")
      } else {
        txtArray[i].changeColor = false;
        console.log("false")
      }
      

    }
    this.setData({
      txtArray: txtArray

    })

  },


  

  charge: function () {
    console.log('开始')
    wx.showLoading({
      title: '加载中',
    })
    var that = this
    wx.request({
      url: that.data.servers + 'user/commitCoachInfo.do',
      data: {
        "data": {
          "userId": this.data.userId,
          "packageId": this.data.flagId
        }
      },
      method: 'POST',
      header: {
        "Content-Type": "application/json"
      },
      success: function (res) {
        console.log(res);
        if (res.data.rtnCode == '0000000'){
          wx.request({
            url: that.data.servers + 'wxPay/commitOrder.do',
            "data": {
              "data": {
                "userId": that.data.userId,
                "orderNo": res.data.bizData.orderNo,
                "orderType": 3
              }

            },
            method: 'POST',
            header: {
              "Content-Type": "application/json"
            },
            success: function (res) {
              console.log(res);
              wx.requestPayment({
                timeStamp: res.data.bizData.paymap.timeStamp,
                nonceStr: res.data.bizData.paymap.nonceStr,
                package: res.data.bizData.paymap.package,
                signType: res.data.bizData.paymap.signType,
                paySign: res.data.bizData.paymap.paySign,
                success: function (res) {
                  wx.hideLoading();
                  wx.showModal({
                    title: '提示',
                    content: '购买成功，请添加客服微信(微信号：s7vii2017)获取后续服务',
                    showCancel: false,
                    success: function (res) {
                      if (res.confirm) {
                        wx.redirectTo({
                          url: '../me/me'
                        })
                      }
                    }
                  })

                },
                fail: function (err) {
                  wx.hideLoading();
                  wx.showModal({
                    title: '提示',
                    content: '遇到了点问题，请稍后重试',
                    showCancel: false
                  })

                }
              })
            },
            fail: function (err) {
              wx.hideLoading();
              wx.showModal({
                title: '提示',
                content: '遇到了点问题，请稍后重试',
                showCancel: false
              })
            }
          })
        }else{
          wx.hideLoading();
          wx.showModal({
            title: '提示',
            content: res.data.msg,
            showCancel: false
          })
        }
      },
      fail: function (err) {
        wx.hideLoading();
        wx.showModal({
          title: '提示',
          content: '遇到了点问题，请稍后重试',
          showCancel: false
        })
      }

    })
  },

  onLoad: function (options) {
    var that = this
    var servers = getApp().data.servsers;
    that.setData({
      servers: servers
    })
    var storeId = options.id;
    console.log(options.id);
    wx.getStorage({
      key: 'userId',
      success: function (res) {
        that.setData({
          userId: res.data
        })
        wx.showLoading({
          title: '加载中',
        })
      }

    })

    wx.request({
      url: that.data.servers + 'common/getCoachPackages.do',
      data: {

      },
      method: 'POST',
      header: {
        "Content-Type": "application/json"
      },
      success: function (res) {
        wx.hideLoading();
        console.log(res.data.bizData);
        that.setData({
          txtArray: res.data.bizData
        });
        var txtArray = that.data.txtArray;
        for (var i = 0; i < that.data.txtArray.length; i++) {

          if (txtArray[i].disPrice == 0) {
            if (txtArray[i].num == 1){
              txtArray[i].txt1 = "体验课程";
            }else{
              txtArray[i].txt1 = txtArray[i].num + " 节课程" +  "\n";
            }
             
          } else {
            txtArray[i].txt1 = txtArray[i].num + " 节课程" + "\n"
            txtArray[i].txt2 =  "立减 " + txtArray[i].disPrice + " 元";
          }
        }

        //设置默认选中的状态
        txtArray[2].changeColor = true;
        var flagId = 3;
        var money = txtArray[2].totalPrice;
        var unitPrice = txtArray[2].totalPrice / txtArray[2].num;
        that.setData({
          txtArray: txtArray,
          flagId: flagId,
          money: money,
          unitPrice: unitPrice
        })
      },
      fail: function (err) {
        wx.hideLoading();
        wx.showModal({
          title: '提示',
          content: '遇到了点问题，请稍后重试',
          showCancel: false
        })
      }
    })



    


  }
})