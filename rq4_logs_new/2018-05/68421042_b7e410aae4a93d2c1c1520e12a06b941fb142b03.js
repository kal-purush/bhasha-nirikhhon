class ClassNameBuilder {
  constructor(className) {
    this._classNames = className ? [className] : [];
  }
  add(className, condition = true) {
    if (condition) {
      this._classNames.push(className);
    }
  }
  get className() {
    return this._classNames.join(' ');
  }
}

export default ClassNameBuilder;