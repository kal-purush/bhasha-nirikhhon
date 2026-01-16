import { createAction, props } from '@ngrx/store';

export const enum SNACKBARSTATUS {
  SUCCESS = 'snackbar-success',
  ERROR = 'snackbar-error',
  INFO = 'snackbar-info',
}

export const snackbarSuccess = createAction(
  '[Notification] snackbarSuccess',
  props<{ message: string }>()
);

export const snackbarFail = createAction(
  '[Notification] snackbarFail',
  props<{ message: string }>()
);

export const snackbarInfo = createAction(
  '[Notification] snackbarInfo',
  props<{ message: string }>()
);