import { Action } from '@ngrx/store';

export enum CoursesActionTypes {
  LoadCourses = '[Courses] Load Coursess',
  LoadCoursesError = '[Courses] Load Coursess Error',
  LoadCoursesSuccess = '[Courses] Load Coursess Success',
}

export class LoadCourses implements Action {
  readonly type = CoursesActionTypes.LoadCourses;
  constructor(public  start, public count, public searchText) {}

}
export class LoadCoursesError implements Action {
  readonly type = CoursesActionTypes.LoadCoursesError;
  constructor(public  error: any) {}
}
export class LoadCoursesSuccess implements Action {
  readonly type = CoursesActionTypes.LoadCoursesSuccess;
  constructor(public  courses: any) {}
}

export type CoursesActions = LoadCourses | LoadCoursesError | LoadCoursesSuccess;