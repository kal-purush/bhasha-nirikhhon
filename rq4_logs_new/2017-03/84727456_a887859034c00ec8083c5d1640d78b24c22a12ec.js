/**
 * Created by admin on 2017/3/12.
 * Author:August913
 */
window.onload=function () {
  new Vue({
    el:'#search',
    data:{
      msg:[],
      title:'',
      now:0
    },
    methods:{
      get :function(ev){
        if(ev.keyCode===38 || ev.keyCode===40)return;
        if(ev.keyCode===13){
          window.open('https://www.baidu.com/s?wd='+this.title);
          this.title='';
        }
        this.$http.jsonp('https://sp0.baidu.com/5a1Fazu8AA54nxGko9WTAnF6hhy/su',{
          params:{
            wd:this.title
          },
          jsonp:'cb'
        }).then(function (res) {
          this.msg=res.data.s
        },function (res) {
          alert(res.status);
        })
      },
      click:function () {
        window.open('https://www.baidu.com/s?wd='+this.title);
        this.title='';
      },
      changeDown :function(){
        this.now++;
        if(this.now==this.msg.length)this.now=0;
        this.title=this.msg[this.now]
      },
      changeUp :function (ev) {
        this.now--;
        if (this.now==-1)this.now=this.msg.length;
        ev.preventDefault();
        this.title=this.msg[this.now]
      },
      hidden: function () {
        this.msg=''
      }

    }
  })
};
/*搜索列表接口
 https://sp0.baidu.com/5a1Fazu8AA54nxGko9WTAnF6hhy/su?wd=a&cb=hello*/
/* 搜索结果网页接口
 https://www.baidu.com/s?wd=b
 */