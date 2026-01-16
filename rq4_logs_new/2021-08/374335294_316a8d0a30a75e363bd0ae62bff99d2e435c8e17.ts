import { CxAsset, CxEntry, CxRequest } from '@proc7ts/context-values';
import { Supply } from '@proc7ts/supply';
import { Formation } from '../formation';
import { Hub } from '../hub';
import { OrderSubject, OrderTask } from '../order';
import { Unit } from './unit';
import { UnitContext } from './unit-context';
import { Unit$Deployment } from './unit.deployment.impl';

export class Unit$OrderSubject<TUnit extends Unit> implements OrderSubject<TUnit> {

  readonly #deployment: Unit$Deployment<TUnit>;
  readonly #supply: Supply;
  #exec = this.#doExec;
  readonly #execute = (task: OrderTask<TUnit>): void => this.#exec(task);

  constructor(backend: Unit$Deployment<TUnit>, supply: Supply) {
    this.#deployment = backend;
    this.#supply = supply.whenOff(reason => this.#exec = this.#dontExec(reason));
  }

  get hub(): Hub {
    return this.context.hub;
  }

  get formation(): Formation {
    return this.context.formation;
  }

  get unit(): TUnit {
    return this.context.unit;
  }

  get context(): UnitContext<TUnit> {
    return this.#deployment.context;
  }

  get supply(): Supply {
    return this.#supply;
  }

  get<TValue>(entry: CxEntry<TValue, unknown>, request?: CxRequest.WithoutFallback<TValue>): TValue;
  get<TValue>(entry: CxEntry<TValue, unknown>, request: CxRequest.WithFallback<TValue>): TValue;
  get<TValue>(entry: CxEntry<TValue, unknown>, request?: CxRequest<TValue>): TValue | null;

  get<TValue>(entry: CxEntry<TValue, unknown>, request?: CxRequest<TValue>): TValue | null {
    return this.context.get(entry, request);
  }

  provide<TValue, TAsset = TValue>(asset: CxAsset<TValue, TAsset, UnitContext<TUnit>>): Supply {
    return this.#deployment.builder.provide(asset).needs(this.supply);
  }

  execute(task: OrderTask<TUnit>): void {
    this.#execute(task);
  }

  #doExec(task: OrderTask<TUnit>): void {

    const { host } = this.#deployment;

    host.workbench.execute(async () => {
      try {
        await host.executeTask(this.unit, task);
      } catch (error) {
        host.log.error(`Failed to execute ${this.unit} task`, error);
        this.supply.off(error);
      }
    });
  }

  #dontExec(error: unknown): () => Promise<void> {
    return () => Promise.reject(error);
  }

}