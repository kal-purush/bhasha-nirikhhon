import { Supply } from '@proc7ts/supply';
import { Formation } from '../formation';
import { FormationManager, Hub } from '../hub';
import { FormationTest } from './formation-test';
import { HubTest } from './hub-test';
import { HubTest$Instance } from './hub-test.instance.impl';
import { OrderTest } from './order-test';
import { OrderTest$Instance } from './order-test.instace.impl';

export class FormationTest$Nested extends OrderTest$Instance implements FormationTest {

  readonly #hubTest: HubTest;
  readonly #hubFormation: Formation;

  constructor(
      hubTest: HubTest$Instance,
      formation: Formation,
      init?: FormationTest.Init,
  ) {
    super(FormationTest$init(hubTest, formation, init));
    this.#hubTest = hubTest;
    this.#hubFormation = formation;
  }

  init(): this {
    // Establish communication.
    void this.#hubTest.formationBuilder.get(FormationManager).formationCtl(this.#hubFormation).channel;

    return this;
  }

}

function FormationTest$init(
    hubTest: HubTest$Instance,
    formation: Formation,
    init: FormationTest.Init = {},
): OrderTest.Init {

  const { supply = new Supply() } = init;

  return {
    ...init,
    supply: supply.needs(hubTest),
    createOrigin(order) {
      return {
        hub: new Hub({ id: hubTest.hub.uid, order }),
        formation: new Formation({ id: formation.uid, order }),
      };
    },
  };
}