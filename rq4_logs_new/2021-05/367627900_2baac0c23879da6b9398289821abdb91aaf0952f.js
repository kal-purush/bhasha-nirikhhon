/**
 * @description: 小球抛物线动画，起点元素和目标元素必须定位
 * @param {HTMLElement} el 起点元素 传入一个html元素
 * @param {HTMLElement} target 目标元素 传入一个html元素
 * @param {Number} duration 运动的时间，默认500毫秒
 * @param {Function} movement 运动的函数，默认的函数为贝塞尔曲线函数cubic-bezier(0.49, -0.29, 0.75, 0.41)
 * @param {Function} callback 运动后执行的回调函数，this指向该对象
 * @param {Function} stepCallback 运动过程中执行的回调函数，this指向该对象，接受x，y参数，分别表示X，Y轴的偏移位置
 * @param {Boolean} autostart 是否自动开始运动，默认为false
 * @return {*}
 */
class Parabola {
  constructor({
    el,
    target,
    duration,
    movement,
    callback,
    stepCallback,
    autostart,
  }) {
    // 起点元素
    this.el = el;
    // 目标元素
    this.target = target;
    // 运动时间
    this.duration = duration || 500;
    // 运动函数
    this.movement = movement || "cubic-bezier(0.49, -0.29, 0.75, 0.41)";
    // 运动后执行的回调函数
    this.callback = callback || function () {};
    // 运动过程中执行的回调函数
    this.stepCallback = stepCallback || function (x, y) {};
    // 是否自动开始运动
    this.autostart = autostart || false;
    // 判断起点元素是否自动运动
    if (this.autostart) this.start();
  }
  // 运动函数，开始执行动画
  start() {
    // 获取起点元素的位置
    let { left: elX, top: elY } = this.el.getBoundingClientRect();
    // 获取目标元素的位置
    let { left: targetX, top: targetY } = this.target.getBoundingClientRect();
    // 计算起点元素的偏移位置
    let offset = [targetX - elX, targetY - elY];
    if (elX) offset[0] = targetX;
    if (elY) offset[1] = targetY;
    // 记录起点元素的初始位置,只会记录第一次
    if (!this.initialization) this.initialization = [elX, elY];
    // 小球运动
    this.el.style.cssText = `top:${offset[1]}px;left:${offset[0]}px;transition: left ${this.duration}ms linear,top ${this.duration}ms ${this.movement};`;
    // 监听起点元素运动
    this.el.addEventListener(
      "transitionrun",
      this.stepCallback.call(this, offset[0], offset[1])
    );
    // 监听起点元素运动结束
    this.el.addEventListener("transitionend", this.callback.call(this));
  }
  // 重置函数,重置元素的位置
  reset() {
    this.el.style.cssText = `top:${this.initialization[1]}px;left:${this.initialization[0]}px`;
  }
  // 停止函数,停止动画
  stop() {
    let { left, top } = this.el.getBoundingClientRect();
    this.el.style.cssText = `top:${top}px;left:${left}px;`;
  }
}