import { Injectable, resolve, Instantiable } from "./testInversify";
import { makeObservable, observable, action } from "mobx";
// import { Container } from "inversify";

// const container2 = new Container({ autoBindInjectable: true });
@Injectable(undefined, true)
class Store1 {
  constructor() {
    makeObservable(this, {
      count: observable,
      increase: action,
    });
  }
  count: number = 0;
  increase = () => {
    this.count += 1;
  };
}

@Injectable()
class Store2 {
  constructor() {
    makeObservable(this, {
      count2: observable,
      increase2: action,
    });
  }
  count2: number = 0;

  increase2 = () => {
    this.count2 += 2;
  };
}

@Injectable()
class Service1 {
  constructor(private store1: Store1) {}

  invoke = () => {
    this.store1.increase();
    console.log("[Service1]", this.store1.count);
  };
  get getCount() {
    return this.store1.count;
  }
}

@Injectable()
class Service2 {
  constructor(private store2: Store2) {}
  invoke = () => {
    this.store2.increase2();
    console.log("[Service2]", this.store2.count2);
  };
  get getCount() {
    return this.store2.count2;
  }
}

test("Resolve", () => {
  const instance = resolve(Service1);
  expect(instance instanceof Service1).toBe(true);
  instance.invoke();
  expect(instance.getCount).toBe(1);
});

test("Resolve 2 layer", () => {
  @Instantiable()
  class Controller1 {
    constructor(private service1: Service1, private service2: Service2) {}
    invoke1 = () => {
      this.service1.invoke();
    };
    invoke2 = () => {
      this.service2.invoke();
    };
    get service1Count() {
      return this.service1.getCount;
    }
    get service2Count() {
      return this.service2.getCount;
    }
  }
  const instance = resolve(Controller1);
  expect(instance instanceof Controller1).toBeTruthy();
  instance.invoke1();
  instance.invoke2();
  expect(instance.service1Count).toBe(2);
  expect(instance.service2Count).toBe(2);
});

test("Circle require", () => {});

test("Instance registration", () => {});