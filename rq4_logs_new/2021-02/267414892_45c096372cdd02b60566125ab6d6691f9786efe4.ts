import { action } from 'typesafe-actions';
import { ActionTypes } from './constants';

export function getAll() {
  return action(ActionTypes.GET_ALL);
}

export function getAllSuccess(payload: any) {
  return action(ActionTypes.GET_ALL_SUCCESS, payload);
}

export function getAllFailed(payload: any) {
  return action(ActionTypes.GET_ALL_FAILED, payload);
}

export function create(payload: any) {
  return action(ActionTypes.MENTOR_CREATE, payload);
}

export function createSuccess(payload: any) {
  return action(ActionTypes.MENTOR_CREATE_SUCCESS, payload);
}

export function createFailed() {
  return action(ActionTypes.MENTOR_CREATE_FAILED);
}

export function update(payload: any) {
  return action(ActionTypes.MENTOR_UPDATE, payload);
}

export function updateSuccess(payload: any) {
  return action(ActionTypes.MENTOR_UPDATE_SUCCESS, payload);
}

export function updateFailed() {
  return action(ActionTypes.MENTOR_UPDATE_FAILED);
}

export function remove(id: string) {
  return action(ActionTypes.MENTOR_REMOVE, id);
}

export function removeSuccess() {
  return action(ActionTypes.MENTOR_REMOVE_SUCCESS);
}

export function removeFailed() {
  return action(ActionTypes.MENTOR_REMOVE_FAILED);
}

export function getById(id: string) {
  return action(ActionTypes.MENTOR_GET_BY_ID, id);
}

export function getByIdSuccess(payload: any) {
  return action(ActionTypes.MENTOR_GET_BY_ID_SUCCESS, payload);
}

export function getByIdFailed() {
  return action(ActionTypes.MENTOR_GET_BY_ID_FAILED);
}