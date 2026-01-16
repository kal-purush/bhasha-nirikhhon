import { ECharts } from 'echarts'
import { Component, Vue } from 'vue-property-decorator'

@Component({
  name: 'ResizeMixin'
})
export default class extends Vue {
  protected chart!: ECharts | null
  private sidebarElm?: Element


  mounted(): void {
    // 添加window 和 sidebar的resize 事件
    this.initResizeEvent()
    this.initSidebarResizeEvent()
  }
  beforeDestroy(): void {
    this.destroyResizeEvent()
    this.destroySidebarResizeEvent()
  }

  activated(): void {
    this.initResizeEvent()
    this.initSidebarResizeEvent()
  }

  deactivated(): void {
    this.destroyResizeEvent()
    this.destroySidebarResizeEvent()
  }

  private chartResizeHandler() {
    if (this.chart) {
      this.chart.resize()
    }
  }
  // sidebar事件
  private sidebarResizeHandler(e: TransitionEvent) {
    if (e.propertyName === 'width') {
      this.chartResizeHandler()
    }
  }
  // 初始化window resize事件
  private initResizeEvent() {
    if (this.chartResizeHandler) {
      window.addEventListener('resize', this.chartResizeHandler)
    }
  }
  // 销毁window resize事件
  private destroyResizeEvent() {
    if (this.chartResizeHandler) {
      window.removeEventListener('resize', this.chartResizeHandler)
    }
  }
  // 初始化sidebar的transitionend事件
  private initSidebarResizeEvent() {
    this.sidebarElm = document.getElementsByClassName('sidebar-container')[0]
    if (this.sidebarElm) {
      this.sidebarElm.addEventListener('transitionend', this.sidebarResizeHandler as EventListener)
    }
  }
  // 销毁sidebar的transitionend事件
  private destroySidebarResizeEvent() {
    if (this.sidebarElm) {
      this.sidebarElm.removeEventListener('transitionend', this.sidebarResizeHandler as EventListener)
    }
  }
}