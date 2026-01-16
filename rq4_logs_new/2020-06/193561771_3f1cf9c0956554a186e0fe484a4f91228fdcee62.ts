import * as inventoryActions from '../actions/inventory-action.creator';
import { reducer, initialState } from './inventory.reducer';
import { inventoryFixture, inventoryDocsFixture, inventoryTargetsDocFixture } from '../models/campaign-state.factory';

describe('Inventory Reducer', () => {
  it('ignores unknown actions', () => {
    const result = reducer(initialState, {});
    expect(result).toBe(initialState);
  });

  it('sets inventory on load success', () => {
    const state = reducer(initialState, inventoryActions.InventoryLoadSuccess({ docs: inventoryDocsFixture }));
    expect(Object.keys(state.entities)).toEqual(['1', '2']);
    expect(state.entities['1']).toMatchObject(inventoryFixture[0]);
    expect(state.entities['2']).toMatchObject(inventoryFixture[1]);
  });

  it('sets and clears inventory load errors', () => {
    let state = reducer(initialState, inventoryActions.InventoryLoadFailure({ error: 'something bad' }));
    expect(state.error).toEqual('something bad');
    state = reducer(state, inventoryActions.InventoryLoadSuccess({ docs: [] }));
    expect(state.error).toBeNull();
  });

  it('sets inventory targets on load success', () => {
    let state = reducer(initialState, inventoryActions.InventoryLoadSuccess({ docs: inventoryDocsFixture }));
    expect(Object.keys(state.entities)).toEqual(['1', '2']);
    expect(state.entities['1'].episodeTargets).toBeUndefined();
    expect(state.entities['1'].countryTargets).toBeUndefined();
    expect(state.entities['2'].episodeTargets).toBeUndefined();
    expect(state.entities['2'].countryTargets).toBeUndefined();

    state = reducer(state, inventoryActions.InventoryTargetsLoadSuccess({ doc: inventoryTargetsDocFixture }));
    expect(inventoryTargetsDocFixture['inventoryId']).toEqual(1);
    expect(state.entities['1'].episodeTargets).toEqual([{ code: '1111', label: 'Episode 1', type: 'episode' }]);
    expect(state.entities['1'].countryTargets).toEqual([{ code: 'CA', label: 'Canadia', type: 'country' }]);
    expect(state.entities['2'].episodeTargets).toBeUndefined();
    expect(state.entities['2'].countryTargets).toBeUndefined();
  });

  it('sets and clears inventory targets load errors', () => {
    let state = reducer(initialState, inventoryActions.InventoryTargetsLoadFailure({ error: 'something bad' }));
    expect(state.error).toEqual('something bad');
    state = reducer(state, inventoryActions.InventoryTargetsLoadSuccess({ doc: inventoryTargetsDocFixture }));
    expect(state.error).toBeNull();
  });
});