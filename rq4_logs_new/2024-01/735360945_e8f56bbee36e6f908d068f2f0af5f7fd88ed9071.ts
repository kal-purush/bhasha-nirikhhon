import { ofType } from "redux-observable";
import { EMPTY, catchError, mergeMap, of } from "rxjs";
import { API } from "../../api/API";
import { Epic } from "./epic";
import { CreateTaskType, State } from "../../utils/types";
import { ProjectActions } from "../slices/project.slice";
import { showFailToastMessage } from "../../main";

export const getStatesEpic: Epic = (action$, state$) =>
  action$.pipe(
    ofType(ProjectActions.getStates.type),
    mergeMap((action) => {
      return API.Board.getStates(action.payload).pipe(
        mergeMap((res) => {
          return of(ProjectActions.setStates(res.response as State[]));
        }),
        catchError(() => {
          showFailToastMessage("There was an error");
          return EMPTY;
        })
      );
    })
  );

export const createTaskEpic: Epic = (action$, state$) =>
  action$.pipe(
    ofType(ProjectActions.createTask.type),
    mergeMap((action) => {
      const taskInfo = action.payload as CreateTaskType;
      return API.Board.createTask(
        taskInfo.id,
        taskInfo.state_id,
        taskInfo.title,
        taskInfo.back_ground_color,
        taskInfo.description
      ).pipe(
        mergeMap((res) => {
          return of(
            ProjectActions.getState({
              stateId: taskInfo.state_id,
              projId: taskInfo.id,
            })
          );
        }),
        catchError(() => {
          showFailToastMessage("There was an error");
          return EMPTY;
        })
      );
    })
  );

export const getStateEpic: Epic = (action$, state$) =>
  action$.pipe(
    ofType(ProjectActions.getState.type),
    mergeMap((action) => {
      return API.Board.getState(
        action.payload.projId,
        action.payload.stateId
      ).pipe(
        mergeMap((res) => {
          return of(ProjectActions.setState(res.response as State));
        }),
        catchError(() => {
          showFailToastMessage("There was an error");
          return EMPTY;
        })
      );
    })
  );

export const editTaskEpic: Epic = (action$, state$) =>
  action$.pipe(
    ofType(ProjectActions.editTask.type),
    mergeMap((action) => {
      const taskInfo = action.payload as CreateTaskType & { task_id: string };
      return API.Board.editTask(
        taskInfo.id,
        taskInfo.task_id,
        taskInfo.title,
        taskInfo.back_ground_color,
        taskInfo.description
      ).pipe(
        mergeMap(() => {
          return of(
            ProjectActions.getState({
              stateId: taskInfo.state_id,
              projId: taskInfo.id,
            })
          );
        }),
        catchError(() => {
          showFailToastMessage("There was an error");
          return EMPTY;
        })
      );
    })
  );