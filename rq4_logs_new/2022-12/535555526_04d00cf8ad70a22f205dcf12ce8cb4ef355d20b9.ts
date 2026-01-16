import "reflect-metadata";
import {
  makeObservable,
  makeAutoObservable,
  observable,
  computed,
  action,
} from "mobx";

type ClassLike<T> = new (...args: any[]) => T;

const Injectable = (): ClassDecorator => (target) => {
  console.warn("[Injectable]", target);
};

const Param =
  (name: string) =>
  (...args: any[]) => {
    console.warn("Params", name, args);
  };

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

class Store2 {
  constructor() {
    makeObservable(this, {
      count: observable,
      increase2: action,
    });
  }
  count: number = 0;

  increase2 = () => {
    this.count += 2;
  };
}

@Injectable()
class Service1 {
  constructor(
    @Param("aaa") private store1: Store1,
    @Param("bbb") private store2: Store2
  ) {}

  increase1 = () => {
    this.store1.increase();
    console.log("Store1", this.store1.count);
  };
  increase2 = () => {
    this.store2.increase2();
    console.log("Store2", this.store2.count);
  };
}

const factory = <T>(target: ClassLike<T>) => {
  const paramTypes = Reflect.getMetadata("design:paramtypes", target);
  console.warn({
    paramTypes,
  });
  if (!paramTypes) return new target();
  const args = paramTypes.map((provider: ClassLike<any>) => factory(provider));
  return new target(...args);
};
const service1 = factory(Service1);
service1.increase1();
service1.increase2();