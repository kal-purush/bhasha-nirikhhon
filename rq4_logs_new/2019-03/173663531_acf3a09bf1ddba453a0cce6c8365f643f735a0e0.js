// 组件的复用(分页)
const paginationBev = Behavior({
  data:{
    // 分页里的全部数据
    dataArray:[],
    total: null,
    // noneResult没有数据
    noneResult:false
  },
  methods:{
    setMoreData(dataArray){
      // concat合并数据
      const tempArray = 
          this.data.dataArray.concat(dataArray)
      this.setData({
        dataArray: tempArray
      })
    },
    // 起始的登录数
    getCurrentStat(){
      return this.data.dataArray.length
    },
    // 全部的数据
    setTotal(total){
      this.data.total = total
      if (total==0){
        this.setData({
          noneResult:true
        })
      }
    },
    // 是否还有更多的数据需要加载
    hasMore(){
      // this.data.dataArray.length >= this.data.total没有更多的数据要加载
      if(this.data.dataArray.length >= this.data.total){
        return false
      }else{
        return true    
      }              
    },
    // 清空数据 
    initialize(){
      this.setData({
        dataArray:[],
        noneResult:false,
        loading:false
      })
      this.data.total=null
    },

    // 小巧函数的封装

    // 判断是否加了锁，是否锁住的状态
    isLocked() {
      return this.data.loading ? true : false
    },
    // 上锁
    locked() {
      this.setData({
        loading:true
      })
    },
    // 解锁
    unLocked() {
      this.setData({
        loading:false
      })
    }
  }
})
export { paginationBev }