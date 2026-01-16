import { Action } from '@ngrx/store';
import { Update } from '@ngrx/entity';
import { DataElementGroup } from '../../models/data-element-group.model';

export enum DataElementGroupActionTypes {
  LoadDataElementGroups = '[DataElementGroup] Load DataElementGroups',
  LoadDataElementGroupsFail = '[DataElementGroup] Load DataElementGroups fail',
  LoadDataElementGroupsInitiated = '[DataElementGroup] Load DataElementGroups initiated',
  AddDataElementGroup = '[DataElementGroup] Add DataElementGroup',
  UpsertDataElementGroup = '[DataElementGroup] Upsert DataElementGroup',
  AddDataElementGroups = '[DataElementGroup] Add DataElementGroups',
  UpsertDataElementGroups = '[DataElementGroup] Upsert DataElementGroups',
  UpdateDataElementGroup = '[DataElementGroup] Update DataElementGroup',
  UpdateDataElementGroups = '[DataElementGroup] Update DataElementGroups',
  DeleteDataElementGroup = '[DataElementGroup] Delete DataElementGroup',
  DeleteDataElementGroups = '[DataElementGroup] Delete DataElementGroups',
  ClearDataElementGroups = '[DataElementGroup] Clear DataElementGroups'
}

export class LoadDataElementGroups implements Action {
  readonly type = DataElementGroupActionTypes.LoadDataElementGroups;
}

export class LoadDataElementGroupsInitiated implements Action {
  readonly type = DataElementGroupActionTypes.LoadDataElementGroupsInitiated;
}

export class LoadDataElementGroupsFail implements Action {
  readonly type = DataElementGroupActionTypes.LoadDataElementGroupsFail;
  constructor(public error: any) {}
}

export class AddDataElementGroup implements Action {
  readonly type = DataElementGroupActionTypes.AddDataElementGroup;

  constructor(public payload: { dataElementGroup: DataElementGroup }) {}
}

export class UpsertDataElementGroup implements Action {
  readonly type = DataElementGroupActionTypes.UpsertDataElementGroup;

  constructor(public payload: { dataElementGroup: DataElementGroup }) {}
}

export class AddDataElementGroups implements Action {
  readonly type = DataElementGroupActionTypes.AddDataElementGroups;

  constructor(public dataElementGroups: DataElementGroup[]) {}
}

export class UpsertDataElementGroups implements Action {
  readonly type = DataElementGroupActionTypes.UpsertDataElementGroups;

  constructor(public payload: { dataElementGroups: DataElementGroup[] }) {}
}

export class UpdateDataElementGroup implements Action {
  readonly type = DataElementGroupActionTypes.UpdateDataElementGroup;

  constructor(public payload: { dataElementGroup: Update<DataElementGroup> }) {}
}

export class UpdateDataElementGroups implements Action {
  readonly type = DataElementGroupActionTypes.UpdateDataElementGroups;

  constructor(
    public payload: { dataElementGroups: Update<DataElementGroup>[] }
  ) {}
}

export class DeleteDataElementGroup implements Action {
  readonly type = DataElementGroupActionTypes.DeleteDataElementGroup;

  constructor(public payload: { id: string }) {}
}

export class DeleteDataElementGroups implements Action {
  readonly type = DataElementGroupActionTypes.DeleteDataElementGroups;

  constructor(public payload: { ids: string[] }) {}
}

export class ClearDataElementGroups implements Action {
  readonly type = DataElementGroupActionTypes.ClearDataElementGroups;
}

export type DataElementGroupActions =
  | LoadDataElementGroups
  | LoadDataElementGroupsInitiated
  | LoadDataElementGroupsFail
  | AddDataElementGroup
  | UpsertDataElementGroup
  | AddDataElementGroups
  | UpsertDataElementGroups
  | UpdateDataElementGroup
  | UpdateDataElementGroups
  | DeleteDataElementGroup
  | DeleteDataElementGroups
  | ClearDataElementGroups;