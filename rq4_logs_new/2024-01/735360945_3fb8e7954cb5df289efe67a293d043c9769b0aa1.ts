import { ofType } from "redux-observable";
import { AuthActions } from "../slices/auth.slice";
import { merge, mergeMap, of } from "rxjs";
import { API } from "../../api/API";
import { Epic, handleError } from "./epic";
import {
  AuthState,
  CreateProject,
  RequestState,
  SignupForm,
  SmallProject,
} from "../../utils/types";
import { ReqActions } from "../slices/req.slice";
import { ProjectActions } from "../slices/project.slice";

export const createProjectEpic: Epic = (action$, state$) =>
  action$.pipe(
    ofType(ProjectActions.createProject.type),
    mergeMap((action) => {
      const createProjectInfo = action as CreateProject;
      return API.createProject(
        createProjectInfo.title,
        createProjectInfo.description,
        createProjectInfo.picture
      ).pipe(
        mergeMap((res) => {            
          window.location.replace("/project/" + (res.response as any).project_id);
          return of(ReqActions.setState({ requestState: RequestState.None }));
        }),
        handleError()
      );
    })
  );