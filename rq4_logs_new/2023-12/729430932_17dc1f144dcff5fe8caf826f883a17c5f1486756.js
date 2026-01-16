export const clientHeightNow = {
  data () {
    return {
      myClientHeight: document.documentElement.clientHeight + 'px'
    }
  },
  mounted () {
    this.updateWindowHeight()
    window.addEventListener('resize', this.updateWindowHeight) // 监听窗口高度变化
  },
  beforeDestroy () {
    window.removeEventListener('resize', this.updateWindowHeight)
  },
  methods: {
    updateWindowHeight () {
      this.myClientHeight = document.documentElement.clientHeight + 'px'
    }
  }
}