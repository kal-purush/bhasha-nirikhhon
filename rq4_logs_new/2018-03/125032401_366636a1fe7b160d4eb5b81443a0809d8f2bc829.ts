import {cat, IObjectRef, meta, action, op} from 'phovea_core/src/provenance';
import {VegaView} from './VegaView';

export const CMD_SET_STATE = 'vegaSetState';

export async function setStateImpl(inputs: IObjectRef<any>[], parameter: any) {
  const view = await inputs[0].v;
  const old = view.setStateImpl(parameter.state);
  return {
    inverse: setState(inputs[0], old)
  };
}

export function setState(view: IObjectRef<VegaView>, state: any) {
  return action(meta('Change State', cat.visual, op.update), CMD_SET_STATE, setStateImpl, [view], {
    state
  });
}