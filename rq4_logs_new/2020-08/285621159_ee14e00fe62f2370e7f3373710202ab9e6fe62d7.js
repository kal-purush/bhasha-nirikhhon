let Vue;
const forEach = (obj, callback) => {
  Object.keys(obj).forEach((key) => {
    callback(key, obj[key]);
  });
}
class Store {
  constructor(options){
    this.vm = new Vue({
      data:{
        state: options.state
      }
    });

    // getters
    let getters = options.getters;
    this.getters = {}; // 给store的实例上添加getters属性

    // 将state中的值以数据绑定的方式添加到getters属性上
    forEach(getters, (getterName, value) => {
      Object.defineProperty(this.getters, getterName, {
        get: () => {
          return value(this.state);
        }
      });
    });

    // mutations
    let mutations = options.mutations;
    this.mutations = {};

    // 采用发布订阅模式
    // 先订阅mutations中定义的操作
    forEach(mutations, (mutationName, value) => {
      // value是mutations中的执行函数
      // 这里不是将value函数直接返回赋值，而是返回一个新函数
      // 这种方式叫做切片编程
      this.mutations[mutationName] = (payload) => {
        // do other things...
        value(this.state, payload)
      }
    });

    // actions
    let actions = options.actions;
    this.actions = {};

    forEach(actions, (actionName, value) => {
      this.actions[actionName] = (payload) => {
        value(this, payload);
      }
    })
  }
  get state(){ // store的state属性
    return this.vm.state
  }
  // 给store实例添加commit方法
  commit = (mutationName, payload) => { // es7的语法，使commit中的this指向当前store的实例
    // 执行发布订阅中的订阅
    this.mutations[mutationName](payload);
  }
  dispatch = (actionName, payload) => {
    this.actions[actionName](payload);
  }
}
const install = (_Vue) => {
  Vue = _Vue;
  // 给调用的Vue实例混入一个beforeCreate生命周期
  Vue.mixin({
    beforeCreate() {
      if(this.$options.store) { // 根组件
        this.$store = this.$options.store;
      } else { // 非 根组件
        this.$store = this.$parent && this.$parent.$options.store;
      }
    },
  })
}
export default {
  install,
  Store
}