/**
 * 1. 计算可视区域的起始数据索引
 * 2. 计算可视区的域结束索引
 * 3. 计算起始数据索引对应的数据在视图中的偏移位置
 * 
 * TODO:
 *  1. 实现不固定高度的虚拟列表
 */

class VirtualList {
  constructor(view, opts) {
    this.view = view
    this.contentBox = this.view.querySelector('.list-view-content')
    this.data = opts.data ||[]
    this.visibleData = []
    this.itemHeight = 30 // 固定元素高度

    this.init()
  }

  init() {
    this.updateVisibleData(this.view)
    this.updateDOM()
    
    this.contentBox.style.height = this.totalHeight + 'px'
    
   this.view.addEventListener('scroll', (e) => {
    this.updateVisibleData(e)
    this.updateDOM()
   })
  }

  updateVisibleData(e) {
    const scrollTop = this.view.scrollTop
    const clientHeight = this.view.clientHeight
    const VisibleNum = Math.ceil(clientHeight / this.itemHeight)
    const startIdx = Math.floor(scrollTop / this.itemHeight)
    const endIdx = startIdx + VisibleNum
    this.visibleData = this.data.slice(startIdx, endIdx + 1)
    this.offset = startIdx
    console.log(this.visibleData)
  }

  updateDOM() {
    this.contentBox.innerHTML = ''
    this.visibleData.forEach( (item, idx) => {
      const li = document.createElement('li')
      li.className = 'list-view-item'
      li.style.transform = `translateY(${(idx + this.offset) * this.itemHeight}px)`
      li.textContent = item
      this.contentBox.appendChild(li)
    })
  }

  add(data) {
    this.data = data
  }

  get totalHeight() { // 整个列表的高度, 用来撑开滚动条
    return this.data.length * this.itemHeight
  }
}