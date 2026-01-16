import axios from 'axios';
import { ICrudGetAction, ICrudGetAllAction, ICrudPutAction, ICrudDeleteAction } from 'react-jhipster';

import { cleanEntity } from 'app/shared/util/entity-utils';
import { REQUEST, SUCCESS, FAILURE } from 'app/shared/reducers/action-type.util';

import { IAutheur, defaultValue } from 'app/shared/model/autheur.model';

export const ACTION_TYPES = {
  FETCH_AUTHEUR_LIST: 'autheur/FETCH_AUTHEUR_LIST',
  FETCH_AUTHEUR: 'autheur/FETCH_AUTHEUR',
  CREATE_AUTHEUR: 'autheur/CREATE_AUTHEUR',
  UPDATE_AUTHEUR: 'autheur/UPDATE_AUTHEUR',
  DELETE_AUTHEUR: 'autheur/DELETE_AUTHEUR',
  RESET: 'autheur/RESET',
};

const initialState = {
  loading: false,
  errorMessage: null,
  entities: [] as ReadonlyArray<IAutheur>,
  entity: defaultValue,
  updating: false,
  updateSuccess: false,
};

export type AutheurState = Readonly<typeof initialState>;

// Reducer

export default (state: AutheurState = initialState, action): AutheurState => {
  switch (action.type) {
    case REQUEST(ACTION_TYPES.FETCH_AUTHEUR_LIST):
    case REQUEST(ACTION_TYPES.FETCH_AUTHEUR):
      return {
        ...state,
        errorMessage: null,
        updateSuccess: false,
        loading: true,
      };
    case REQUEST(ACTION_TYPES.CREATE_AUTHEUR):
    case REQUEST(ACTION_TYPES.UPDATE_AUTHEUR):
    case REQUEST(ACTION_TYPES.DELETE_AUTHEUR):
      return {
        ...state,
        errorMessage: null,
        updateSuccess: false,
        updating: true,
      };
    case FAILURE(ACTION_TYPES.FETCH_AUTHEUR_LIST):
    case FAILURE(ACTION_TYPES.FETCH_AUTHEUR):
    case FAILURE(ACTION_TYPES.CREATE_AUTHEUR):
    case FAILURE(ACTION_TYPES.UPDATE_AUTHEUR):
    case FAILURE(ACTION_TYPES.DELETE_AUTHEUR):
      return {
        ...state,
        loading: false,
        updating: false,
        updateSuccess: false,
        errorMessage: action.payload,
      };
    case SUCCESS(ACTION_TYPES.FETCH_AUTHEUR_LIST):
      return {
        ...state,
        loading: false,
        entities: action.payload.data,
      };
    case SUCCESS(ACTION_TYPES.FETCH_AUTHEUR):
      return {
        ...state,
        loading: false,
        entity: action.payload.data,
      };
    case SUCCESS(ACTION_TYPES.CREATE_AUTHEUR):
    case SUCCESS(ACTION_TYPES.UPDATE_AUTHEUR):
      return {
        ...state,
        updating: false,
        updateSuccess: true,
        entity: action.payload.data,
      };
    case SUCCESS(ACTION_TYPES.DELETE_AUTHEUR):
      return {
        ...state,
        updating: false,
        updateSuccess: true,
        entity: {},
      };
    case ACTION_TYPES.RESET:
      return {
        ...initialState,
      };
    default:
      return state;
  }
};

const apiUrl = 'api/autheurs';

// Actions

export const getEntities: ICrudGetAllAction<IAutheur> = (page, size, sort) => ({
  type: ACTION_TYPES.FETCH_AUTHEUR_LIST,
  payload: axios.get<IAutheur>(`${apiUrl}?cacheBuster=${new Date().getTime()}`),
});

export const getEntity: ICrudGetAction<IAutheur> = id => {
  const requestUrl = `${apiUrl}/${id}`;
  return {
    type: ACTION_TYPES.FETCH_AUTHEUR,
    payload: axios.get<IAutheur>(requestUrl),
  };
};

export const createEntity: ICrudPutAction<IAutheur> = entity => async dispatch => {
  const result = await dispatch({
    type: ACTION_TYPES.CREATE_AUTHEUR,
    payload: axios.post(apiUrl, cleanEntity(entity)),
  });
  dispatch(getEntities());
  return result;
};

export const updateEntity: ICrudPutAction<IAutheur> = entity => async dispatch => {
  const result = await dispatch({
    type: ACTION_TYPES.UPDATE_AUTHEUR,
    payload: axios.put(apiUrl, cleanEntity(entity)),
  });
  return result;
};

export const deleteEntity: ICrudDeleteAction<IAutheur> = id => async dispatch => {
  const requestUrl = `${apiUrl}/${id}`;
  const result = await dispatch({
    type: ACTION_TYPES.DELETE_AUTHEUR,
    payload: axios.delete(requestUrl),
  });
  dispatch(getEntities());
  return result;
};

export const reset = () => ({
  type: ACTION_TYPES.RESET,
});