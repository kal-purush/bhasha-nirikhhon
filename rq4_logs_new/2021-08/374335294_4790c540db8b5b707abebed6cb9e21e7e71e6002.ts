import { AfterEvent, mapAfter, trackValue, ValueTracker } from '@proc7ts/fun-events';
import { Formation } from '../formation';
import { Unit } from '../unit';
import { Unit$Backend__symbol } from '../unit/unit.backend.impl';
import { Formation$Host } from './formation.host';

export class Unit$DeploymentTracker<TUnit extends Unit = Unit> {

  readonly read: AfterEvent<[ReadonlyMap<string, Formation>]>;
  readonly #tracker: ValueTracker<[Map<string, Formation>]>;

  constructor(readonly host: Formation$Host, readonly unit: TUnit) {

    const { asFormation } = unit;

    this.#tracker = trackValue([
        asFormation ? new Map([[asFormation.uid, asFormation]]) : new Map(),
    ]);
    this.read = this.#tracker.read.do(
        mapAfter(([formations]) => formations),
    );
  }

  deployTo(formation: Formation): void {

    const [formations] = this.#tracker.it;

    this.#tracker.it = [formations.set(formation.uid, formation)];
    if (this.host.formation.uid === formation.uid) {
      this.unit[Unit$Backend__symbol].deployTo(this.host.formation);
    }
  }

}