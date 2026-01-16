import Options from './options';
import ServiceProviderClass from './serviceProviderClass';

/**
 * Used internally to allow the optional inject syntax
 */
export default class Injectable {
  constructor(
    private module: ng.IModule,
    private dependencies?: any[],
    private options?: Options
  ) { }

  /**
   * Registers a class as an Angular service
   */
  service = (target: Function) => {
    this.classInject(target);
    this.module.service(target.name, target);
  }

  /**
   * Registers a class as an Angular provider
   */
  provider<T>() {
    return (target: ServiceProviderClass<T>) => {
      this.classInject(target, 'Provider');
      this.module.provider(target.name, target);
    };
  }

  /**
   * Registers a class as an Angular controller
   */
  controller = (target: Function) => {
    this.classInject(target);
    this.module.controller(target.name, target);
  }

  /**
   * Registers a class as an Angular component.
   * Automatically sets the class as the controller.
   */
  component(options?: ng.IComponentOptions, name?: string) {
    return (target: any) => {
      this.classInject(target);
      options = options || {};
      options.controller = target;
      this.module
        .controller(target.name, target)
        .component(name || this.options.prefix + target.name, options);
    };
  }

  /**
   * Declares a directive using the provided dependencies
   */
  directive(fn: ng.IDirectiveFactory, name?: string) {
    if (!name && !fn.name) {
      throw 'Directives should be declared with a named function or you should explicitly provide a name.';
    }
    this.module.directive(name || this.options.prefix + fn.name, this.functionInject(fn));
  }

  /**
   * Declares a configuration block using the provided dependencies.
   * If classes are passed as dependency, they need to have been declared as providers.
   */
  config(fn: Function) {
    this.module.config(this.functionInject(fn, 'Provider'));
  }

  /**
   * Declares a run block using the provided dependencies.
   */
  run(fn: Function) {
    this.module.run(this.functionInject(fn));
  }

  /**
   * Declares an Angular filter using the provided dependencies.
   */
  filter(fn: Function, name?: string) {
    name = name || fn.name;
    if (!name) {
      throw 'Filters should be declared with a named function or you should explicitly provide a name.';
    }
    this.module.filter(name, this.functionInject(fn));
  }

  private functionInject(target: Function, postfix?: string) {
    if (this.dependencies && this.dependencies.length) {
      return this.injectables(postfix).concat(target);
    }
    return target;
  }

  private classInject(target: Function, postfix?: string) {
    if (this.dependencies && this.dependencies.length) {
      target.$inject = this.injectables(postfix);
    }
  }

  private injectables(postfix?: string) {
    postfix = postfix || '';
    return this.dependencies.map(s => s.name ? s.name + postfix : s);
  }
}