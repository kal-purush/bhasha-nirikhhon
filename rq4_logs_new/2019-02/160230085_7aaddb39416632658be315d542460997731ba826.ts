
export function HideLoader(triggerAction = '') {
  return function (Class: Function) {
    Object.defineProperty(Class.prototype, 'triggerAction', {
      value: triggerAction
    });
  }
}