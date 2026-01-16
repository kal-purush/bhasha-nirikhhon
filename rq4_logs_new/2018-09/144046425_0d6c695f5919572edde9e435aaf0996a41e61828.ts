import {Action} from '@ngrx/store';
import {Project} from "../models/project";

export enum ProjectsListActionTypes {
  Select = '[ProjectsList] Select',
}

export class Select implements Action {
  readonly type = ProjectsListActionTypes.Select;

  constructor(public payload: string) {
  }
}

export type ProjectsListActionsUnion =
| Select;