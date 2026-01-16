import 'reflect-metadata';
import { Container, injectable } from 'inversify';

const CLASS_SYMBOL = Symbol('classSymbol');
const container = new Container();

type ClassLike<T> = new (...args: any[]) => T;

const Injectable = (name?: string, singleton?: boolean) => {
  return (target: ClassLike<any>) => {
    let classIdentifier = Symbol.for(target.name);
    if (typeof name !== 'undefined') {
      classIdentifier = Symbol(name);
      Reflect.defineMetadata(CLASS_SYMBOL, classIdentifier, target);
    }
    const bind = container.bind<typeof target>(classIdentifier).to(target);
    if (singleton) bind.inSingletonScope();
    injectable()(target);
  };
};

const getClassSymbol = (target: ClassLike<any>) => {
  const storedSymbol = Reflect.getMetadata(CLASS_SYMBOL, target);
  return storedSymbol ?? Symbol.for(target.name);
};

const resolve = <T extends any>(target: ClassLike<T>) => {
  const paramTypes = Reflect.getMetadata('design:paramtypes', target);
  if (!paramTypes) return container.resolve(target);
  const args: any[] = (paramTypes as any[]).map((_target) => resolve(_target));
  return new target(...args);
};