import { createAction, props, union } from '@ngrx/store';
import { ActionTypes } from './action.types';
import { HalDoc } from 'ngx-prx-styleguide';

export const InventoryLoad = createAction(ActionTypes.CAMPAIGN_INVENTORY_LOAD);
export const InventoryLoadSuccess = createAction(ActionTypes.CAMPAIGN_INVENTORY_LOAD_SUCCESS, props<{ docs: HalDoc[] }>());
export const InventoryLoadFailure = createAction(ActionTypes.CAMPAIGN_INVENTORY_LOAD_FAILURE, props<{ error: any }>());

export const InventoryTargetsLoad = createAction(ActionTypes.CAMPAIGN_INVENTORY_TARGETS_LOAD, props<{ inventoryId: number }>());
export const InventoryTargetsLoadSuccess = createAction(ActionTypes.CAMPAIGN_INVENTORY_TARGETS_LOAD_SUCCESS, props<{ doc: HalDoc }>());
export const InventoryTargetsLoadFailure = createAction(ActionTypes.CAMPAIGN_INVENTORY_TARGETS_LOAD_FAILURE, props<{ error: any }>());

const all = union({
  InventoryLoad,
  InventoryLoadSuccess,
  InventoryLoadFailure,
  InventoryTargetsLoad,
  InventoryTargetsLoadSuccess,
  InventoryTargetsLoadFailure
});
export type InventoryActions = typeof all;