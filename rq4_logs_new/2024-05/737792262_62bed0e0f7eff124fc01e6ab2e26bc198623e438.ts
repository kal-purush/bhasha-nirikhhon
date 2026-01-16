/* TypeScript 装饰器  */
/* 
type Constructor<T = {}> = new (...args: any[]) => T;

class InstanceCollector {
  instances = new Set();

  install = <Class extends Constructor>(Value: Class, context: ClassDecoratorContext<Class>) => {
    const _this = this;
    return class extends Value {
      constructor(...args: any[]) {
        super(...args);
        _this.instances.add(this);
      }
    };
  };
}

const collector = new InstanceCollector();

@collector.install
class Calculator {
  add(a: number, b: number): number {
    return a + b;
  }
}

const calculator1 = new Calculator();
const calculator2 = new Calculator();

console.log("instances: ", collector.instances);
 */
function log<This, Args extends any[], Return>(target: (this: This, ...args: Args) => Return, context: ClassMethodDecoratorContext<This, (this: This, ...args: Args) => Return>) {
  const methodName = String(context.name);

  function replacementMethod(this: This, ...args: Args): Return {
    console.log(`LOG: Entering method '${methodName}'.`);
    const result = target.call(this, ...args);
    console.log(`LOG: Exiting method '${methodName}'.`);
    return result;
  }

  return replacementMethod;
}

class Calculator {
  @log
  add(a: number, b: number): number {
    return a + b;
  }
}

const calculator = new Calculator();
console.log(calculator.add(5, 3));