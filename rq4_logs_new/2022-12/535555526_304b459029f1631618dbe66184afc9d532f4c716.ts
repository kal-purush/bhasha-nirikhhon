import "reflect-metadata";
import { Container, injectable, inject } from "inversify";
import { makeObservable, observable, action } from "mobx";
import { ClassLike, Injectable, container, resolve } from "./testInversify";

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

// container.bind(Store1).to(Store1).inSingletonScope();
// container.bind(Store2).to(Store2);

@injectable()
class Service1 {
  public store1: Store1;
  constructor(@inject(Store1) store1: Store1) {
    this.store1 = store1;
  }

  invoke = () => {
    this.store1.increase();
  };
}

@injectable()
class Service2 {
  public store1: Store1;
  public store2: Store2;
  constructor(@inject(Store1) store1: Store1, @inject(Store2) store2: Store2) {
    this.store1 = store1;
    this.store2 = store2;
  }

  invoke = () => {
    this.store1.increase();
    this.store2.increase2();
  };
}

const storeIns1 = resolve(Store1);
storeIns1.increase();

const storeIns2 = resolve(Store1);
console.log(storeIns2.count);