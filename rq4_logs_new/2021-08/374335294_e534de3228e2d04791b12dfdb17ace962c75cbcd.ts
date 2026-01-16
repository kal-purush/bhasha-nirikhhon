import { CxEntry, cxScoped, cxSingle } from '@proc7ts/context-values';
import { CommChannel, CommProcessor } from '../communication';
import { Formation, FormationContext } from '../formation';

export interface FormationStarter {

  startFormation(formation: Formation, options: FormationStartOptions): CommChannel;

}

export interface FormationStartOptions {

  readonly processor: CommProcessor;

}

export const FormationStarter: CxEntry<FormationStarter> = {
  perContext: (/*#__PURE__*/ cxScoped(
      FormationContext,
      (/*#__PURE__*/ cxSingle()),
  )),
  toString: () => '[FormationStarter]',
};