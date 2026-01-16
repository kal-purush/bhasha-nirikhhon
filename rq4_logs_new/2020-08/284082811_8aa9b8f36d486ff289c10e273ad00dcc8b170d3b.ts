import BindingEngine from './BindingEngine';
import Notifiable, { NotifiableClass } from './Notifiable';

@NotifiableClass
class Root {
  private _description: string = '';
  private _total: number = 0;
  private _child?: Child;
  private _notNotifiable: boolean = true;

  @Notifiable
  public get description(): string {
    return this._description;
  }
  public set description(value: string) {
    this._description = value;
  }

  @Notifiable
  public get total(): number {
    return this._total;
  }
  public set total(value: number) {
    this._total = value;
  }

  @Notifiable
  public get child(): Child | undefined {
    return this._child;
  }
  public set child(value: Child | undefined) {
    this._child = value;
  }

  public get notNotifiable(): boolean {
    return this._notNotifiable;
  }
  public set notNotifiable(value: boolean) {
    this._notNotifiable = value;
  }
}

@NotifiableClass
class Child {
  private _name: string = '';
  private _isReady: boolean = false;

  @Notifiable
  public get name(): string {
    return this._name;
  }
  public set name(value: string) {
    this._name = value;
  }

  @Notifiable
  public get isReady(): boolean {
    return this._isReady;
  }
  public set isReady(value: boolean) {
    this._isReady = value;
  }
}

describe('BindingEngine', () => {
  let root: Root;
  let engine: BindingEngine;

  beforeEach(() => {
    root = new Root();
    engine = new BindingEngine(root);
  });

  it('executes function bound to property path that changes', () => {
    const execute: jest.Mock = jest.fn();
    engine.addBinding('this.description', execute);

    const newValue: string = 'new description';
    root.description = newValue;

    expect(execute).toHaveBeenCalledWith(newValue);
  });

  it('does not execute function bound to path that did not change', () => {
    const execute: jest.Mock = jest.fn();
    engine.addBinding('this.description', execute);

    root.total = 10;

    expect(execute).not.toHaveBeenCalled();
  });

  it('can bind multiple functions to the same path', () => {
    const execute1: jest.Mock = jest.fn();
    const execute2: jest.Mock = jest.fn();
    engine.addBinding('this.total', execute1);
    engine.addBinding('this.total', execute2);

    const newValue: number = -5;
    root.total = newValue;

    expect(execute1).toHaveBeenCalledWith(newValue);
    expect(execute2).toHaveBeenCalledWith(newValue);
  });

  it('will not execute bindings on non-notifiable properties', () => {
    const execute: jest.Mock = jest.fn();
    engine.addBinding('this.notNotifiable', execute);

    root.notNotifiable = false;

    expect(execute).not.toHaveBeenCalled();
  });

  it('can add bindings to properties that are currently undefined', () => {
    const execute: jest.Mock = jest.fn();
    engine.addBinding('this.child', execute);

    const newValue: Child = new Child();
    root.child = newValue;

    expect(execute).toHaveBeenCalledWith(newValue);
  });

  it('will execute bindings on properties that become undefined', () => {
    root.child = new Child();
    const execute: jest.Mock = jest.fn();
    engine.addBinding('this.child', execute);

    root.child = undefined;

    expect(execute).toHaveBeenCalledWith(undefined);
  });

  it('can add bindings to child properties that are notifiable', () => {
    root.child = new Child();
    const execName: jest.Mock = jest.fn();
    const execChild: jest.Mock = jest.fn();
    engine.addBinding('this.child.name', execName);
    engine.addBinding('this.child', execChild);

    const newValue: string = 'Something';
    root.child.name = newValue;

    expect(execName).toHaveBeenCalledWith(newValue);
    expect(execChild).not.toHaveBeenCalled();
  });

  it('will execute bindings on child properties when child changes', () => {
    const execIsReady: jest.Mock = jest.fn();
    const execChild: jest.Mock = jest.fn();
    engine.addBinding('this.child.isReady', execIsReady);
    engine.addBinding('this.child', execChild);

    const newValue: boolean = true;
    const child: Child = new Child();
    child.isReady = newValue;
    root.child = child;

    expect(execIsReady).toHaveBeenCalledWith(newValue);
    expect(execChild).toHaveBeenCalledWith(child);
  });

  it('will not execute bindings on child properties if child changes to undefined', () => {
    root.child = new Child();
    const execIsReady: jest.Mock = jest.fn();
    const execChild: jest.Mock = jest.fn();
    engine.addBinding('this.child.isReady', execIsReady);
    engine.addBinding('this.child', execChild);

    root.child = undefined;

    expect(execIsReady).not.toHaveBeenCalled();
    expect(execChild).toHaveBeenCalledWith(undefined);
  });

  it('unsubscribes from propertyChanged events on old child properties', () => {
    const originalChild: Child = new Child();
    root.child = originalChild;
    const execute: jest.Mock = jest.fn();
    engine.addBinding('this.child.name', execute);

    root.child = new Child();
    originalChild.name = 'New name';

    expect(execute).toHaveBeenCalledTimes(1); // not twice
  });
});