export class Eventer {

  /**
   */
  constructor() {
    this._events = {};

  }

  /**
   *  @description Подписка на событие
   * @param  {String} name Имя события
   * @param {Function} func callback Функция
   */
  on(name, func) {
    this._create(name);
    this._events[name].push(func)
  }

  /**
   * @description Запуск события
   * @param {String}name
   * @param {*?}data
   */
  emit(name, data) {
    this._create(name);
    this._events[name].forEach(f => f(data))
  }

  /**
   * @description Отписка от события
   * @param {String}name
   * @param {Function?}func
   */
  off(name, func) {
    if (this._events[name]) {
      if (func) {
        while (-1 < this._events[name].indexOf(func)) {
          this._events[name].splice(this._events[name].indexOf(func), 1)
        }
      } else {
        this._events[name].length = 0;
      }
    }
  }

  get events() {
    return this._events
  }


  destroy() {
    Object.keys(this._events).forEach(name => {
      delete this._events[name]
    })
  }

  _create(name) {
    !this._events[name] && (this._events[name] = []);
  }
}


/**
 * @param {Eventer}eventer
 * @param {Array<string>}eventNameList
 * @return {function(*)}
 */
export function eventdecorator(eventer, eventNameList) {
  return (TargetClass) => {
    TargetClass = class M extends TargetClass {
      constructor(...args) {
        super(...args)
        this.state = this.state || {}
        const setStateProp = (key, value) => {
          const obj = {}
          obj[key] = value
          this.setState(obj)
        }

        eventNameList.forEach(name => {
          this.state[name] = null
          eventer.on(name, (data) => {
            setStateProp(name, data)
          })
        })
      }

      componentWillUnmount() {
        eventNameList.forEach(name => {
          eventer.off(name)
        })
        super.componentWillUnmount && super.componentWillUnmount()
      }
    }
    return TargetClass
  }

}


