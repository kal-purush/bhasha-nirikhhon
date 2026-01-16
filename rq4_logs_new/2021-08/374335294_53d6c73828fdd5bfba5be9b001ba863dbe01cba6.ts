import { Supply } from '@proc7ts/supply';
import { Formation } from '../formation';
import { OrderSubject, OrderTask } from '../order';
import { Unit } from './unit';
import { Unit$Backend } from './unit.backend.impl';
import { Unit$Host } from './unit.host.impl';

export class Unit$Backend$OrderSubject<TUnit extends Unit, THost extends Unit$Host = Unit$Host>
    implements OrderSubject<TUnit> {

  readonly #backend: Unit$Backend<TUnit, THost>;
  readonly #supply: Supply;
  #exec = this.#doExec;
  readonly #execute = (task: OrderTask<TUnit>): void => this.#exec(task);

  constructor(backend: Unit$Backend<TUnit, THost>, supply: Supply) {
    this.#backend = backend;
    this.#supply = supply;
    this.#supply.whenOff(reason => this.#exec = this.#dontExec(reason));
  }

  get formation(): Formation {
    return this.#backend.host.formation;
  }

  get unit(): TUnit {
    return this.#backend.unit;
  }

  get supply(): Supply {
    return this.#supply;
  }

  get execute(): (task: OrderTask<TUnit>) => void {
    return this.#execute;
  }

  #doExec(task: OrderTask<TUnit>): void {

    const { host } = this.#backend;

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